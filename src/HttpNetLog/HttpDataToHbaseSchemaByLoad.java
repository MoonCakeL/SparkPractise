package HttpNetLog;

import HttpNetLog.utils.GetHttpNetDataType;
import HttpNetLog.utils.UUIDUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.spark.FamilyHFileWriteOptions;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.spark.KeyFamilyQualifier;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sparkHbase.example.utils.HbaseConnectFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class HttpDataToHbaseSchemaByLoad {
	private static final Logger logger = LoggerFactory.getLogger(HttpDataToHbaseSchemaByLoad.class);

	public static void hbaseByLoadGo()/*main(String[] args)*/{
		SparkConf conf = new SparkConf().setAppName("hbaseByLoadGo");
		//.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String[]> rdd = sc.textFile("hdfs://192.168.42.24:9000/data/ncmdp_08500001_Net_20130515164000.txt")
				.map( x -> (x.split("\t")));

		HttpDataToHbaseSchemaByLoad dataToHbaseByLoad = new HttpDataToHbaseSchemaByLoad();
		String hbaseTable = "hbaseHttpByload";
		dataToHbaseByLoad.saveByLoad(rdd,hbaseTable);

	}

	public void saveByLoad(JavaRDD<String[]> rdd,String hbaseTable){

		TableName tableName = TableName.valueOf(hbaseTable);
		HttpDataToHbaseSchemaByPut dataToHbase = new HttpDataToHbaseSchemaByPut();
		//检查、建表
		dataToHbase.createTable(hbaseTable);

		//rowkey设计：
		// 8个长度的UUid + "-" 1 + 按照用户收集号码|ip15为长度 + 16为长度 共24位
		//rdd.foreach();
		JavaRDD<String[]> hbaseRDD = rdd.flatMap(
				x ->{
					String uuidKey = UUIDUtil.getRowKeyUUID();

					List<String[]> returnList = new ArrayList<>();

					if (x.length >0 && x !=null){
						//前两位固定是rowKey和列簇，List中byte数组需要从0开始添加，占用了两位
						//循环从i=2开始
						// rdd下标从0开始取，所以后面循环i-2
						//rdd长度29+2=31-2=29 x.length=29
						for (int i=2;i<x.length+2; i++){
							String[] bytes = new String[4];
							//添加rowKey
							bytes[0]=(uuidKey + x[2]);
							//列簇：
							bytes[1]=(GetHttpNetDataType.ColumnFamily);
							//列名
							bytes[2]=(GetHttpNetDataType.httpNetLogColumn.get(i-2).toString());
							//value
							bytes[3]=(x[i-2]);
							returnList.add(bytes);
						}
						logger.info("循环结束后returnList大小："+returnList.size());

					}
					//最后需要迭代返回输出，迭代的是bytesList中的每一个byte[]的对象
					//所以需要外面在添加一个数组存放这些byte[]对象

					return returnList.iterator();
				}
		);
		/*
			Following the Spark bulk load command, use the HBase’s LoadIncrementalHFiles object to load
			the newly created HFiles into HBase.

				Additional Parameters for Bulk Loading with Spark
					You can set the following attributes with additional parameter options on hbaseBulkLoad.

					Max file size of the HFiles

					A flag to exclude HFiles from compactions

					Column Family settings for compression, bloomType, blockSize, and dataBlockEncoding
					*/
		//调用LoadFuncition对数据rdd生成HFile
		try {
			Configuration conf = HbaseConnectFactory.getInstance().getHBaseConfiguration();
			JavaHBaseContext hbaseContext = new JavaHBaseContext(
					//通过传入进来的rdd就带着SparkConf，创建JavaSparkContext
					JavaSparkContext.fromSparkContext(rdd.context())
					,conf);
			logger.info("HBaseContext实例化完毕...");
			try {
				/*
				The hbaseBulkLoad function takes three required parameters:

					1.The table name of the table we intend to bulk load too

					2.A function that will convert a record in the RDD to a
					  tuple key value par.

					  With the tuple key being a KeyFamilyQualifer object
					  and the value being the cell value.

					  The KeyFamilyQualifer object will hold the RowKey, Column Family, and Column Qualifier.
					  The shuffle will partition on the RowKey but will sort by all three values.

					3.The temporary path for the HFile to be written out too
					*/

				//生成HFile的路径
				String hfilePath = "/data/hfiletmp/"+hbaseTable;
				//生成HFile后，调用LoadIncrementalHFiles方法获取HFile
				Path hfofDir = new Path(hfilePath);
				//每次先删除一下上次的临时目录，确保路径不冲突
				hfofDir.getFileSystem(conf).delete(hfofDir,true);

				//调用生成HFile方法
				hbaseContext.bulkLoad(
						//数据RDD
						hbaseRDD,
						//目标表
						tableName,
						//LoadFunction
						new HttpDataToHbaseSchemaByLoad.BulkLoadFunction(),
						//HFile的存放路径
						hfilePath,
						//With the tuple key being a KeyFamilyQualifer object
						//and the value being the cell value.
						//The KeyFamilyQualifer object will hold the RowKey,
						// Column Family, and Column Qualifier.

						/*
						val familyHBaseWriterOptions =
							new java.util.HashMap[Array[Byte], FamilyHFileWriteOptions]

						val f1Options =
							new FamilyHFileWriteOptions(
								"GZ"（压缩方式）, "ROW", 128（blockSize）, "PREFIX（过滤）")

						familyHBaseWriterOptions.put(Bytes.toBytes("columnFamily1"), f1Options)*/
						new HashMap<byte[], FamilyHFileWriteOptions>(),
						//compactionExclude是否启动压缩
						false,
						//HFile文件大小maxSize
						HConstants.DEFAULT_MAX_FILE_SIZE);

				logger.info("本次完成LoadrowKey：" + hbaseRDD.count() + "条，生成HFile完毕...");

				//调用LoadIncrementalHFiles方法将生成的HFile进行HBase入库

				logger.info("开始将生成的HFile进行对HBase入库...");
				try {
					LoadIncrementalHFiles load = new LoadIncrementalHFiles(conf);

					load.doBulkLoad(hfofDir,
							HbaseConnectFactory.getInstance().getHbaseConnect().getAdmin(),			//获取admin
							HbaseConnectFactory.getInstance().getHbaseConnect().getTable(tableName),	//获取Tbale
							HbaseConnectFactory.getInstance().getHbaseConnect().getRegionLocator(tableName)	//获取要存放在那个HRegionServer
					);

					//完成HFile的load后删除临时目录
					hfofDir.getFileSystem(conf).delete(hfofDir,true);
					logger.info("完成HFile的Load，删除HFile临时目录...");

				}catch (Exception e){

					logger.error("进行HFile时异常，Load失败..."+e.getMessage());

				}finally {
					//完成HFile的load后删除临时目录
					hfofDir.getFileSystem(conf).delete(hfofDir,true);
				}

			}catch (Exception e){
				logger.error("LoadrowKey生成HFile失败..."+e.getMessage());
			}
		}catch (Exception e){
			logger.error("HBaseContext实例化失败..."+e.getMessage());
		}

	}

	public static class BulkLoadFunction implements Function<String[], Pair<KeyFamilyQualifier, byte[]>> {

		@Override
		public Pair<KeyFamilyQualifier, byte[]> call(String[] x) throws Exception {

			if (x == null)
				return null;

			if(x.length != 4)
				return null;
			//为KeyFamilyQualifier对象添加数据row 列簇 列 value
			KeyFamilyQualifier kfq = new KeyFamilyQualifier(Bytes.toBytes(x[0]), Bytes.toBytes(x[1]),
					Bytes.toBytes(x[2]));
			return new Pair(kfq, Bytes.toBytes(x[3]));
		}
	}

}
