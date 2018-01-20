package HttpNetLog;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class HttpDatatoHive {

	/**
	 * 前面经过SparkStreaming进行一个简单过滤做成RDD
	 * 传入进来后进行
	 *基于数据的基本hive入库，然后在做批处理的数据处理。
	 */

	public static void main(String[] args){
		SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<List<String>> rdd = sc.textFile("hdfs://192.168.42.24:9000/data/ncmdp_08500001_Net_20130515164000.txt")
				.map( x -> Arrays.asList(x.split("\t")));

		/*JavaRDD<List<String>> rdd = dataRDD.map(
				x -> Arrays.asList(x.split("\t"))
		);*/

		HttpDatatoHive datatoHive = new HttpDatatoHive();
		datatoHive.save(rdd);
	}

	public void save(JavaRDD<List<String>> javaRDD){
		SparkSession spark = SparkSession.builder()
				.appName("DataSaveToHive")
				.enableHiveSupport()
				.getOrCreate();

		//创建表
		HttpDatatoHive datatoHive = new HttpDatatoHive();
		datatoHive.createTable();

		JavaRDD<HttpNetCaseClass> httpNetRDD = javaRDD.map(
				line -> {

					System.out.println(line.size());

					HttpNetCaseClass httpNetCaseClass = new HttpNetCaseClass();
					httpNetCaseClass.setBeginTime(Long.valueOf(line.get(0)));
					httpNetCaseClass.setEndTime(Long.valueOf(line.get(1)));
					httpNetCaseClass.setMSISDN(String.valueOf(line.get(2)));
					httpNetCaseClass.setSourceIP(String.valueOf(line.get(3)));
					httpNetCaseClass.setSourcePort(String.valueOf(line.get(4)));
					httpNetCaseClass.setAPIP(String.valueOf(line.get(5)));
					httpNetCaseClass.setAPMAC(String.valueOf(line.get(6)));
					httpNetCaseClass.setACIP(String.valueOf(line.get(7)));
					httpNetCaseClass.setACMAC(String.valueOf(line.get(8)));
					httpNetCaseClass.setRequestType(String.valueOf(line.get(9)));
					httpNetCaseClass.setDestinationIP(String.valueOf(line.get(10)));
					httpNetCaseClass.setDestinationPort(String.valueOf(line.get(11)));
					httpNetCaseClass.setService(String.valueOf(line.get(12)));
					httpNetCaseClass.setServiceType1(String.valueOf(line.get(13)));
					httpNetCaseClass.setServiceType2(String.valueOf(line.get(14)));
					httpNetCaseClass.setURL(String.valueOf(line.get(15)));
					httpNetCaseClass.setDomain(String.valueOf(line.get(16)));
					httpNetCaseClass.setSiteName(String.valueOf(line.get(17)));
					httpNetCaseClass.setSiteType1(String.valueOf(line.get(18)));
					httpNetCaseClass.setSiteType2(String.valueOf(line.get(19)));
					httpNetCaseClass.setICP(String.valueOf(line.get(20)));
					httpNetCaseClass.setUpPackNum(String.valueOf(line.get(21)));
					httpNetCaseClass.setDownPackNum(String.valueOf(line.get(22)));
					httpNetCaseClass.setUpPayLoad(String.valueOf(line.get(23)));
					httpNetCaseClass.setDownPayLoad(String.valueOf(line.get(24)));
					httpNetCaseClass.setHttpStatus(String.valueOf(line.get(25)));
					httpNetCaseClass.setUA(String.valueOf(line.get(26)));
					httpNetCaseClass.setClientType(String.valueOf(line.get(27)));
					httpNetCaseClass.setResponseTime(Long.valueOf(line.get(28)));

					return httpNetCaseClass;
				}

		);

		//真是不能用case class 因为它序列化出来的顺序，你是不可控的，和表定义的字段顺序不一致，真是很坑爹
		//白搞了一天，还是编程schema能控制比较好。20180118 10：25
		Dataset<Row> dataDS = spark.createDataFrame(httpNetRDD,HttpNetCaseClass.class);

		//为表增加当天分区字段
		SimpleDateFormat sf = new SimpleDateFormat("yyyyMMdd");

		//DataFrame的API可以接收Column对象,UDF的定义不能直接定义为Scala函数，
		//而是要用定义在org.apache.spark.sql.functions中的udf方法来接收一个函数。
		//这种方式无需register
		//如果需要在函数中传递一个变量，则需要org.apache.spark.sql.functions中的lit函数来帮助
		System.out.println(functions.lit(sf.format(new Date())).toString());
		//使用库
		spark.sql("use httpnetlogdb");
		//追加createDate列到数据中，并赋值
		Dataset<Row> row = dataDS.withColumn("CREATE_DATE",functions.lit(sf.format(new Date())));
		row.show();

		/*真是要死出来的字段顺序和表结构的顺序不一致，case class 真是不好用！！！！
				+--------------+-----+----+--------------------+---------------+------------+--------------------+--------------------+-------------+----------+---------------+---------------+--------------------+-----------+-----------+-------------+----------+-----------+-------------+-------+------------+------------+---------------+---------+---------+------------+----------+---------+---------+-----------+
				|          ACIP|ACMAC|APIP|               APMAC|            ICP|      MSISDN|                  UA|                 URL|    beginTime|clientType|  destinationIP|destinationPort|              domain|downPackNum|downPayLoad|      endTime|httpStatus|requestType| responseTime|service|serviceType1|serviceType2|       siteName|siteType1|siteType2|    sourceIP|sourcePort|upPackNum|upPayLoad|CREATE_DATE|
				+--------------+-----+----+--------------------+---------------+------------+--------------------+--------------------+-------------+----------+---------------+---------------+--------------------+-----------+-----------+-------------+----------+-----------+-------------+-------+------------+------------+---------------+---------+---------+------------+----------+---------+---------+-----------+
				|              |     |    |                    |   111.13.87.17| 10.80.58.44|                    |                    |1368607184116|         2|   111.13.87.17|             80|                    |          0|          0|1368607184116|       200|           |            0|   掌中新浪|        生活软件|          门户|   111.13.87.17|         |         | 10.80.58.44|     35679|        1|       66|   20180118|
				| 183.224.64.98|     |    |0A-1F-6F-36-3F-55...|  119.75.217.56| 15287134073|                    |                    |1368607188107|         2|  119.75.217.56|             80|                    |          0|          0|1368607188107|       200|           |            0|     百度|          搜索|        搜索引擎|  119.75.217.56|         |         |10.80.33.202|     59061|        1|       60|   20180118|
				|183.224.64.130|     |    |0A-1F-6F-36-43-62...|   111.13.12.15| 18213579259|                    |                    |1368607170535|         2|   111.13.12.15|             80|                    |          0|          0|1368607188444|       200|           |            0|     百度|          搜索|        搜索引擎|   111.13.12.15|         |         |10.80.70.247|     56837|        3|      180|   20180118|
				|183.224.64.122|     |    |0A-1F-6F-36-42-46...|  119.75.220.41| 15912595867|                    |                    |1368607183072|         2|  119.75.220.41|             80|                    |          2|        397|1368607184298|       200|           |1368607183153|     百度|          搜索|        搜索引擎|  119.75.220.41|         |         | 10.80.62.98|     52548|        2|      120|   20180118|
				|183.224.64.122|     |    |0A-1F-6F-35-60-1A...|220.181.159.148| 18213875408|                    |                    |1368607186435|         2|220.181.159.148|             80|                    |          0|          0|1368607186435|       200|           |            0|360安全卫士|        工具软件|        安全杀毒|220.181.159.148|         |         | 10.80.63.26|     52750|        1|       66|   20180118|
				|              |     |    |                    | 61.155.219.116| 10.81.87.61|                    |                    |1368607185345|         2| 61.155.219.116|             80|                    |          1|         70|1368607185417|       200|           |            0| PPLIVE|       媒体与视频|       Web视频| 61.155.219.116|         |         | 10.81.87.61|     58061|        1|       60|   20180118|
				|              |     |    |                    |             腾讯|10.80.203.79|Mozilla/4.0 (comp...|http://masterconn...|1368607170517|         2| 117.135.129.68|             80|   masterconn.qq.com|          0|          0|1368607188628|       200|       POST|            0|     QQ|        即时通信|        社交联络|            腾讯网|       门户|     综合门户|10.80.203.79|     59468|        5|     1336|   20180118|
				| 211.139.21.14|     |    |00-27-1d-00-0e-aa...|120.204.200.124| 18785061263|                    |                    |1368607186792|         2|120.204.200.124|             80|                    |          1|         64|1368607186853|       200|           |            0|     QQ|        即时通信|        社交联络|120.204.200.124|         |         | 10.81.50.20|     52942|        1|       60|   20180118|
				| 211.139.21.14|     |    |00-27-1d-00-14-83...|122.226.161.189| 13669734390|                    |                    |1368607183324|         2|122.226.161.189|             80|                    |          2|        178|1368607188719|       200|           |1368607183459| HTTP传输|      IANA标准|      IANA标准|122.226.161.189|         |         |10.81.55.180|     54701|        3|      184|   20180118|
				|              |     |    |                    | 218.30.118.239| 10.81.97.34|Mozilla/4.0 (comp...|                    |1368607188910|         2| 218.30.118.239|             80|intf1.zsall.mobil...|          0|          0|1368607188910|       200|           |            0|360安全卫士|        工具软件|        安全杀毒| 218.30.118.239|         |         | 10.81.97.34|     49452|        1|      501|   20180118|
				|              |     |    |                    | 211.139.29.199|10.80.96.148|                    |                    |1368607188441|         2| 211.139.29.199|           7080|                    |          1|         64|1368607188445|       200|           |            0| HTTP传输|      IANA标准|      IANA标准| 211.139.29.199|         |         |10.80.96.148|     36944|        1|       66|   20180118|
				|              |     |    |                    |   183.224.1.15| 10.80.62.30|                    |                    |1368607184150|         2|   183.224.1.15|             80|                    |          7|       8818|1368607184212|       200|           |1368607184158| HTTP传输|      IANA标准|      IANA标准|   183.224.1.15|         |         | 10.80.62.30|     56780|        5|      330|   20180118|
				| 183.224.64.98|     |    |0A-1F-6F-32-AF-DC...|   111.1.52.242| 15969507820|                    |                    |1368607187653|         2|   111.1.52.242|             80|                    |          1|         70|1368607187678|       200|           |            0| HTTP传输|      IANA标准|      IANA标准|   111.1.52.242|         |         |10.80.36.255|     49470|        1|       60|   20180118|
				| 211.139.21.14|     |    |00-27-1d-06-42-1e...|    14.17.29.41| 18314437582|                    |                    |1368607185216|         2|    14.17.29.41|             80|                    |          5|        352|1368607185470|       200|           |1368607185317|     QQ|        即时通信|        社交联络|    14.17.29.41|         |         | 10.81.49.26|     50619|        2|      120|   20180118|
				|              |     |    |                    |          奇虎360|10.80.102.29|Mozilla/5.0 (comp...|http://cdn.weathe...|1368607178148|         2|    113.31.42.4|             80|cdn.weather.hao.3...|          3|        192|1368607184454|       200|        GET|1368607178245|360安全卫士|        工具软件|        安全杀毒|        360安全中心|      互联网|     信息安全|10.80.102.29|     49273|        4|      909|   20180118|
				| 211.139.21.14|     |    |00-27-1d-01-f5-0a...|             淘宝| 18787021311|Taobao2013/3.0.3 ...|http://itq.i01.wi...|1368607174314|         2| 223.82.136.240|             80|itq.i01.wimg.taob...|         43|      57349|1368607188751|       200|        GET|1368607174402|   央视高清|       媒体与视频|        网络电视|            淘宝网|      互联网|     电子商务| 10.81.55.97|     49244|       10|      945|   20180118|
				| 183.224.64.10|     |    |00-27-1d-01-f1-cf...|211.140.133.230| 18213036338|Mozilla/4.0 (comp...|http://211.140.13...|1368607185648|         1|211.140.133.230|             80|     211.140.133.230|          4|        443|1368607185905|       302|        GET|1368607185709|   上网浏览|        网站访问|        网站访问|211.140.133.230|         |         |10.81.102.48|     52836|        3|      807|   20180118|
				|  183.224.64.9|     |    |00-27-1d-07-db-1b...|    info.wps.cn| 18388405240|         Mozilla/5.0|http://info.wps.c...|1368607184140|         1| 114.112.66.152|             80|         info.wps.cn|          3|        486|1368607184537|       200|        GET|1368607184222|   上网浏览|        网站访问|        网站访问|    info.wps.cn|         |         |10.81.90.189|     51639|        4|      601|   20180118|
				| 183.224.64.10|     |    |00-27-1d-01-f7-af...|             百度| 15887163794|Dalvik/1.4.0 (Lin...|http://c.tieba.ba...|1368607186311|         2|  111.13.12.161|             80|   c.tieba.baidu.com|          0|          0|1368607186517|       200|       POST|            0|     百度|          搜索|        搜索引擎|             百度|       搜索|     搜索引擎|10.81.101.48|     45750|        2|      666|   20180118|
				|183.224.64.122|     |    |0A-1F-6F-36-0C-F8...|  122.13.201.70| 15912133681|                    |                    |1368607184940|         2|  122.13.201.70|             80|                    |          1|         64|1368607184984|       200|           |1368607184984| HTTP传输|      IANA标准|      IANA标准|  122.13.201.70|         |         |10.80.62.215|     57427|        1|       60|   20180118|
								+--------------+-----+----+--------------------+---------------+------------+--------------------+--------------------+-------------+----------+---------------+---------------+--------------------+-----------+-----------+-------------+----------+-----------+-------------+-------+------------+------------+---------------+---------+---------+------------+----------+---------+---------+-----------+
										only showing top 20 rows*/

		// 给表指定分区，没有分区进行指定
		//alter table httpnetlog add if not Exists partition(CREATE_DATE=20180117)
		spark.sql("alter table httpnetlog add if not Exists partition(CREATE_DATE="+ sf.format(new Date()) + ")");

		//分区内写入数据
		//The format of the existing table httpnetlogdb.httpnetlog is `HiveFileFormat`.
		// It doesn't match the specified format `ParquetFileFormat`.;
		//row.write().format("parquet").partitionBy("CREATE_DATE").mode("append").saveAsTable("httpnetlogtest1");
		row.write().format("parquet")/*.partitionBy("CREATE_DATE")*/.mode("append").saveAsTable("httpnetlog");

	}

	public void createTable(){
		SparkSession spark = SparkSession.builder()
				.appName("HivecreateTable")
				.enableHiveSupport()
				.getOrCreate();
		//用于根据时间为hive提供分区
		SimpleDateFormat sf = new SimpleDateFormat("yyyyMMdd");
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
				"partitioned by (CREATE_DATE string) " +  //hive的分区表是物理层面的分区
				"stored as parquet ";  //建表时指定Hive的文件存储格式：Parquet
		//System.out.println(sql);

		spark.sql(sql);

	}

}
