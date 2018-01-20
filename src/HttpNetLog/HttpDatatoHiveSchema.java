package HttpNetLog;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static HttpNetLog.utils.GetHttpNetDataType.*;

public class HttpDatatoHiveSchema {
	private static final Logger logger = LoggerFactory.getLogger(HttpDatatoHiveSchema.class);

	public static void test() /*main(String[] args)*/{
		SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String[]> rdd = sc.textFile("hdfs://192.168.42.24:9000/data/ncmdp_08500001_Net_20130515164000.txt")
				.map( x -> (x.split("\t")));

		HttpDatatoHiveSchema datatoHiveSchema = new HttpDatatoHiveSchema();
		datatoHiveSchema.save(rdd);
	}

	/**
	 * 昨天手写了一个用case class反射做dataset的代码，
	 * 结果真的很坑，case class自动反射出来的字段顺序，居然
	 * 和我定义的顺序不一致，貌似是用ASCII码表通过字母的顺序
	 * 进行排序输出的，和我的表定义完全不一样导致入库失败
	 * 今天就手写schema来固定死顺序在尝试入库。
	 * case class真的是很坑，越方便的东西，越是不可控制。
	 * 20180118 10：49
	 * @param rdd
	 */
	public String save(JavaRDD<String[]> rdd){

		SparkSession spark = SparkSession.builder()
				.appName("DataSaveToHiveSchema")
				.enableHiveSupport()
				.getOrCreate();
		//建表
		HttpDatatoHiveSchema datatoHiveSchema = new HttpDatatoHiveSchema();
		datatoHiveSchema.createTable();

		//对数据进行JavaRDD<Row>创建
		JavaRDD<Row> rowRDD = rdd.map(
				x -> {
					//保存Row对象的list
					List<Object> rowList = new ArrayList<>();
					if (x != null && (x.length > 0)) {

						for (int i = 0; i < x.length; i++) {

							//根据统一下标做成rowList
							rowList.add(
									convertDataType(
											x[i],
											//根据统一下标获取schema数据类型
											ConvertTypetoHive(httpGetDataType.get(i).toString())
									)
							);
						}
					}
				 	return RowFactory.create(rowList.toArray());
					}
		);

		//做schema
		//根据统一下标获取schema字段
		List<StructField> schemaList = new ArrayList<>();
		httpNetLogColumn.forEach((key, value) -> {
				StructField field = DataTypes.createStructField(
						value.toString(),
						ConvertTypetoHive(httpGetDataType.get(key).toString())
						,true
				);
				schemaList.add(field);
		});
		StructType schema = DataTypes.createStructType(schemaList);

		Dataset<Row> httpNetDF = spark.createDataFrame(rowRDD,schema);

		//为表增加当天分区字段
		SimpleDateFormat sf = new SimpleDateFormat("yyyyMMdd");

		/*测试分区使用
		Date today = new Date();
		Calendar c = Calendar.getInstance();
		c.setTime(today);
		c.add(Calendar.DAY_OF_MONTH,1);
		String targetPartition = sf.format(c.getTime());
		System.out.println(targetPartition);*/

		String targetPartition = sf.format((new Date()));

		//DataFrame的API可以接收Column对象,UDF的定义不能直接定义为Scala函数，
		//而是要用定义在org.apache.spark.sql.functions中的udf方法来接收一个函数。
		//这种方式无需register
		//如果需要在函数中传递一个变量，则需要org.apache.spark.sql.functions中的lit函数来帮助
		//使用库
		spark.sql("use httpnetlogdb");
		//追加createDate列到数据中，并赋值
		Dataset<Row> row = httpNetDF.withColumn("CREATE_DATE",functions.lit(targetPartition));
		//row.show();

		// 给表指定分区，没有分区进行指定
		//alter table httpnetlog add if not Exists partition(CREATE_DATE=20180117)
		spark.sql("alter table httpnetlog add if not Exists partition(CREATE_DATE="+ targetPartition + ")");

		//将数据写入hive,直接写hive在hdfs上的全目录路径，因为saveAsTable是个坑，
		//hive有自己的parquet格式，spark也有自己的parquet格式两个格式不一样，、
		//所以直接写hdfs的路径就用spark的parquet格式一样可以查询。
		//The format of the existing table httpnetlogdb.httpnetlog is `HiveFileFormat`.
		//It doesn't match the specified format `ParquetFileFormat`.;
		//row.write().format("parquet").partitionBy("CREATE_DATE").mode("append").saveAsTable("httpnetlog");
		//唉，每一个坑真的要自己趟一遍才行，所以之前用case class的方式应该也是没有问题的，不过还是继续使用编程
		//schema的方式来做dataset比较好，开发中修改一些配置就可以生效，也不用修改case class，也不用序列化了
		//StringBuilder target = new StringBuilder("/hivedata/httpnetlogdb.db/httpnetlog/");
		//target.append(targetPartition);
		//System.out.println(target);
		String target ="/hivedata/httpnetlogdb.db/httpnetlog/";
		logger.info("开始写入hive数据");

		row.write().format("parquet").partitionBy("CREATE_DATE").mode("append").save(target.toString());
		return  "本次hive数据写入已完毕：" + row.count() + "条";
}

	/**
	 * hive建表使用
	 */
	public void createTable(){
		SparkSession spark = SparkSession.builder()
				.appName("HivecreateTableSchema")
				.enableHiveSupport()
				.getOrCreate();
		//创建库
		spark.sql("create database if not exists httpnetlogdb");
		spark.sql("use httpnetlogdb");
		//创建表
		String sql ="create table if not exists httpnetlog (" +
				"BeginTime BIGINT," +
				"EndTime BIGINT," +
				"MSISDN STRING," +
				"SourceIP STRING," +
				"SourcePort STRING," +
				"APIP STRING," +
				"APMAC STRING," +
				"ACIP STRING," +
				"ACMAC STRING," +
				"RequestType STRING," +
				"DestinationIP STRING," +
				"DestinationPort STRING," +
				"Service STRING," +
				"ServiceType1 STRING," +
				"ServiceType2 STRING," +
				"URL STRING," +
				"Domain STRING," +
				"SiteName STRING," +
				"SiteType1 STRING," +
				"SiteType2 STRING," +
				"ICP STRING," +
				"UpPackNum STRING," +
				"DownPackNum STRING," +
				"UpPayLoad STRING," +
				"DownPayLoad STRING," +
				"HttpStatus STRING," +
				"UA STRING," +
				"ClientType STRING," +
				"ResponseTime BIGINT) " +
				"partitioned by (CREATE_DATE string) " +
				"stored as parquet";
				//hive的分区表是物理层面的分区
					//"stored as parquet ";
					// 建表时指定Hive的文件存储格式：Parquet但是hive 有自己的Parquet格式
					//spark 也有自己的Parquet格式两者不一样，但如果在
					// 在写入的时候指定写入格式用spark的Parquet，也必须按照这样的格式建表
					//因为hive如果默认不配置的化，可能在解析读数据的时候不安Parquet去读取数据，
		//会有乱码问题，唉...又是个坑 201801191415
		spark.sql(sql);
		logger.info("完成hive表创建");
	}
}
