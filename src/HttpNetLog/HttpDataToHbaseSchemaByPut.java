package HttpNetLog;

import HttpNetLog.utils.GetHttpNetDataType;
import HttpNetLog.utils.UUIDUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sparkHbase.example.utils.HbaseConnectFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class HttpDataToHbaseSchemaByPut implements Serializable {
	private static final Logger logger = LoggerFactory.getLogger(HttpDataToHbaseSchemaByPut.class);

	public static void /*hbaseByPutGo()*/main(String[] args){
		SparkConf conf = new SparkConf().setAppName("test")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String[]> rdd = sc.textFile("hdfs://192.168.42.24:9000/data/httpnettest.txt")
				.map( x -> (x.split("\t")));

		HttpDataToHbaseSchemaByPut dataToHbase = new HttpDataToHbaseSchemaByPut();
		String hbaseTable = "hbasetest";
		dataToHbase.saveByPut(rdd,hbaseTable);
		//生成HBaseRDD的大小：2993525
	}

	public void saveByPut(JavaRDD<String[]> rdd,String hbaseTable){

		/*Exception in thread "main" org.apache.spark.SparkException:
			Only one SparkContext may be running in this JVM (see SPARK-2243).
			To ignore this error, set spark.driver.allowMultipleContexts = true.
			The currently running SparkContext was created

		一个程序中只能有一个SparkConf，本身上一个调用程序
		传入进来的rdd就带着SparkConf，所以wuxu创建。*/
		//SparkConf sparkConf = new SparkConf().setAppName("HttpDataHBaseSaveByPut");
		//JavaSparkContext jsc = new JavaSparkContext(rdd.context().getConf());

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

		//调用PutFuncition
		try {
				HbaseConnectFactory.getInstance();
				Configuration conf = HbaseConnectFactory.getInstance().getHBaseConfiguration();
				JavaHBaseContext hbaseContext = new JavaHBaseContext(
						//通过传入进来的rdd就带着SparkConf，创建JavaSparkContext
						JavaSparkContext.fromSparkContext(rdd.context())
						,conf);
				logger.info("HBaseContext实例化完毕...");
				try {
					hbaseContext.bulkPut(hbaseRDD,tableName,new PutFunction());
					logger.info("本次完成rowKey：" + hbaseRDD.count() + "条");
				}catch (Exception e){
					logger.error("HBase入库失败...",e.getMessage());
				}
		}catch (Exception e){
			logger.error("HBaseContext实例化失败...",e.getMessage());
		}

	}

	public static class PutFunction implements Function<String[], Put> {
		private static final long serialVersionUID = 1L;

		public Put call(String[] cells) throws Exception {

			//添加rowKey
			Put put = new Put(Bytes.toBytes(cells[0]));
			put.addColumn(
					//添加列簇
					Bytes.toBytes(cells[1]),
					//添加列名
					Bytes.toBytes(cells[2]),
					//添加value
					Bytes.toBytes(cells[3]));
			return put;
		}

	}

	public void createTable(String hbaseTable){

		//懒汉式初始化实例
		Connection conn = HbaseConnectFactory.getInstance().getHbaseConnect();

		try {
			Admin admin = conn.getAdmin();
			TableName tableName = TableName.valueOf(hbaseTable);

			int ttl = 90;

			if (!admin.tableExists(tableName)){
				TableDescriptorBuilder tableDesc =
						TableDescriptorBuilder.newBuilder(tableName);

				ColumnFamilyDescriptorBuilder columnFamilyDesc =
						ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(GetHttpNetDataType.ColumnFamily));
				//每个列族保留最近三个版本的记录
				columnFamilyDesc.setMaxVersions(3);
				//添加列族
				tableDesc.addColumnFamily(columnFamilyDesc.build());
				//设置写WAL日志级别，在hbase数据写入缓存时，同步写WAL，防止因宕机等问题造成数据在内存中丢失
				tableDesc.setDurability(Durability.ASYNC_WAL);
				if(ttl > 0){
					//TTL老化时间
					columnFamilyDesc.setTimeToLive(ttl);
				}

				admin.createTable(tableDesc.build());
				logger.info("完成Hbase创建表，关闭Hbase连接");
				admin.close();
			}

		} catch (IOException e) {
			logger.error("连接Hbase失败: ",e.getMessage());
		}
	}
}
