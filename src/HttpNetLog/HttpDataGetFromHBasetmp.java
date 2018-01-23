package HttpNetLog;

import HttpNetLog.utils.GetHttpNetDataType;
import HttpNetLog.utils.GetHttpNetHBaseType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;
import scala.Tuple2;
import sparkHbase.example.utils.HbaseConnectFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class HttpDataGetFromHBasetmp implements Serializable {
	private static final Logger logger = LoggerFactory.getLogger(HttpDataGetFromHBasetmp.class);

	public static void main(String[] args){

	HttpDataGetFromHBasetmp getFromHBase = new HttpDataGetFromHBasetmp();
		getFromHBase.getHbase();
	}

	/**
	 * 接收需要查询的rowKeyRDD，执行查询后，
	 * 将结果查询完毕后直接封装成Dataset<Row>
	 */
	public void getHbase(/*JavaRDD<byte[]> rowKeyRDD*/){
		SparkConf sparkConf = new SparkConf()
				.setMaster("local")
				.setAppName("scanRowKey");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		JavaRDD<byte[]> rowKeyRDD = jsc.parallelize(Arrays.asList(Bytes.toBytes("bed5e858-10.80.58.44")));
		try {

			TableName tableName = TableName.valueOf("hbasetest");
			Configuration conf = HbaseConnectFactory.getInstance().getHBaseConfiguration();

			JavaHBaseContext hbaseContext = new JavaHBaseContext(
					//同一个spark application中只能有一个SparkConf\SparkContext
					//以及一个JavaSparkContext
					//均可以从传入的RDD中获取，如
					//SparkConf:rowKeyRDD.context().getConf()
					//SparkContext:rowKeyRDD.context()
					//JavaSparkContext:JavaSparkContext.fromSparkContext(rowKeyRDD.context())
					//JavaSparkContext.fromSparkContext(rowKeyRDD.context())
					jsc
					,conf);
			try {
				if (HbaseConnectFactory.getInstance()
						.getHbaseConnect().getAdmin().tableExists(tableName)){

				//将row通过bulkGet方法查询，调用ResultFunction方法，直接返回Dataset<Row>
					JavaRDD<ConcurrentHashMap<String,String>> hbaseDFRDD = hbaseContext.bulkGet(tableName, 2, rowKeyRDD
								, new GetFunction(),
							new ResultFunction());
					logger.info("已完成HBaseGet查询,查询结果为："+ hbaseDFRDD.count() +"条");

					HttpDataGetFromHBasetmp fromHBase = new HttpDataGetFromHBasetmp();
					fromHBase.transformationDF(hbaseDFRDD);
					logger.info("经HBaseGet，入库条为："+hbaseDFRDD.count());

				}

			}catch (Exception e){
				logger.error(e.getMessage());
			}
		}catch (Exception e){
			logger.error("实例化HbaseConntext失败..."+e.getMessage());
		}

	}

	public void transformationDF(JavaRDD<ConcurrentHashMap<String, String>> hbaseRDD) {

		SparkSession spark = SparkSession.builder()
				//.master("local")
				.config(hbaseRDD.context().getConf())
				.appName("CreateDataFrameFromHBase")
				.getOrCreate();

		JavaRDD<Tuple2<List<Row>,StructType>> rowRDD = hbaseRDD.map(
				x -> {

					List<StructField> fields = new ArrayList<>();
					List<Object> objectList = new ArrayList<>();

						for (Map.Entry<String,String> entry : x.entrySet()){
							//row
				    		Object o = GetHttpNetDataType.convertDataType(
								entry.getValue(),
								GetHttpNetHBaseType.hbaseGetDataType.get(entry.getKey()));

				    		//fields
						StructField field = DataTypes.createStructField(
								entry.getKey(),
								GetHttpNetHBaseType.hbaseGetDataType.get(entry.getKey()),
								true
						);


						objectList.add(o);
						fields.add(field);
					}
					List<Row> lists = new ArrayList<>();

					for (Object o: objectList){
						lists.add(RowFactory.create(o));

					};

					StructType schema = DataTypes.createStructType(fields);

					return new Tuple2<>(lists,schema);
				}
		);

		System.out.println("====");
		JavaRDD<Row> r1 = rowRDD.map(
				x -> x._1
		).flatMap(
				new FlatMapFunction<List<Row>, Row>() {
					private static final long serialVersionUID = -5193564823495067011L;

					@Override
					public Iterator<Row> call(List<Row> rows) throws Exception {

						return rows.iterator();
					}
				}
		);

		StructType schema = rowRDD.first()._2;

		r1.foreach(
				x -> System.out.println(x.mkString())
		);

		schema.printTreeString();
		System.out.println("===="+schema.size()+r1.count());
		Dataset<Row> df = spark.createDataFrame(r1,schema);

		df.show();

	}



	public static class GetFunction implements Function<byte[], Get> {

		private static final long serialVersionUID = 1L;

		public Get call(byte[] v) throws Exception {
			return new Get(v);
		}
	}

	public static class ResultFunction implements Function<Result, ConcurrentHashMap<String,String>> {

		private static final long serialVersionUID = 1L;

		/**
		 * Hbase数据转换ConcurrentHashMap算法
		 *
		 * @param result
		 * @return
		 * @throws Exception
		 */

		public ConcurrentHashMap<String, String> call(Result result) throws Exception {

			/*if (result !=null && result.size()>0) {
				System.exit(0);
			}*/

			Iterator<Cell> it = result.listCells().iterator();

			if (!it.hasNext()){
				logger.info("未能查询到数据...");
				System.exit(0);
			}

			ConcurrentHashMap<String, String> hbaseMap = new ConcurrentHashMap<>(29);

			while (it.hasNext()) {
				Cell cell = it.next();

				//获取列名数组
				byte[] qualifierArray = cell.getQualifierArray();
				//获取列数组长度长度
				int qualifierLength = cell.getQualifierLength();
				//获取列名在数组中的下标
				int qualifierOffset = cell.getQualifierOffset();
				//拷贝目标临时数组变量，根据实际列名在数组中长度初始化
				byte[] tmpqualifierArray = new byte[qualifierLength];
				//根据列名数组的下标和长度，在列名数组中，拷贝数组中列名内容
				System.arraycopy(
						//原始数组
						qualifierArray,
						//列名数组下标
						qualifierOffset,
						//拷贝目标临时数组变量
						tmpqualifierArray,
						//在原始数组中起始位置拷贝
						0,
						//从原始数组中起始位置到结束位置长度
						qualifierLength);
				//获取列名
				String qualifier = Bytes.toString(tmpqualifierArray);

				//获取value数组
				byte[] valueArray = cell.getValueArray();
				//长度
				int valuelength = cell.getValueLength();
				//下标
				int valueoffset = cell.getValueOffset();
				//拷贝目标数组变量
				byte[] tempvaluearray = new byte[valuelength];
				System.arraycopy(valueArray, valueoffset, tempvaluearray, 0, valuelength);
				//获取value
				String value = Bytes.toString(tempvaluearray);

				//put
				hbaseMap.put(qualifier, value);
			};
			return hbaseMap;
		}
	}
}
