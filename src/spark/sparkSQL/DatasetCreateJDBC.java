package spark.sparkSQL;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 *
 package org.apache.spark.examples.sql;

 private static void runJdbcDatasetExample(SparkSession spark) {
 // $example on:jdbc_dataset$
 // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
 // Loading data from a JDBC source
 Dataset<Row> jdbcDF = spark.read()
 .format("jdbc")
 .option("url", "jdbc:postgresql:dbserver")
 .option("dbtable", "schema.tablename")
 .option("user", "username")
 .option("password", "password")
 .load();

 Properties connectionProperties = new Properties();
 connectionProperties.put("user", "username");
 connectionProperties.put("password", "password");
 Dataset<Row> jdbcDF2 = spark.read()
 //connectionProperties
 .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);

 // Saving data to a JDBC source
 jdbcDF.write()
 .format("jdbc")
 .option("url", "jdbc:postgresql:dbserver")
 .option("dbtable", "schema.tablename")
 .option("user", "username")
 .option("password", "password")
 .save();

 jdbcDF2.write()
 .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);

 // Specifying create table column data types on write
 jdbcDF.write()
 .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
 .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);
 // $example off:jdbc_dataset$
 }
 }
 */

public class DatasetCreateJDBC {

	public static class BusJavaBean implements Serializable {
		private String	cardID;
		private Integer	lineID;
		private String	beginTime;
		private String	endTime;
		private Integer stationFrom;
		private Integer	stationTo;
		private Integer busId;

		public BusJavaBean() {
		}

		public String getCardID() {
			return cardID;
		}

		public void setCardID(String cardID) {
			this.cardID = cardID;
		}

		public Integer getLineID() {
			return lineID;
		}

		public void setLineID(Integer lineID) {
			this.lineID = lineID;
		}

		public String getBeginTime() {
			return beginTime;
		}

		public void setBeginTime(String beginTime) {
			this.beginTime = beginTime;
		}

		public String getEndTime() {
			return endTime;
		}

		public void setEndTime(String endTime) {
			this.endTime = endTime;
		}

		public Integer getStationFrom() {
			return stationFrom;
		}

		public void setStationFrom(Integer stationFrom) {
			this.stationFrom = stationFrom;
		}

		public Integer getStationTo() {
			return stationTo;
		}

		public void setStationTo(Integer stationTo) {
			this.stationTo = stationTo;
		}

		public Integer getBusId() {
			return busId;
		}

		public void setBusId(Integer busId) {
			this.busId = busId;
		}
	}
		static SparkSession spark = SparkSession.builder()
				.appName("SparkSQL JDBC")
				.master("local")
				.getOrCreate();
	public static void main(String[] args) throws Exception {

		//jdbcSave();
		jdbcLoad();
	}

	public static void jdbcSave(){
	/*mysql> create table busdata(
			    cardID VARCHAR(64),
				lineID INT,
				beginTime VARCHAR(64),
				endTime VARCHAR(64),
				stationFrom INT,
				stationTo INT,
				busID INT);*/
		Map<String,String> mysqlProperties = new HashMap<>();

		mysqlProperties.put("user", "admin");
		mysqlProperties.put("password", "admin");
		mysqlProperties.put("dbtable", "busdata");
		mysqlProperties.put("url", "jdbc:mysql://192.168.42.27:3306/test");

		// Create a JDBC source
		 Dataset<Row> jdbcDF = spark.read()
				.format("jdbc")
				.options(mysqlProperties)
				//.option("url", "jdbc:mysql://192.168.42.27:3306/test")
				//.option("dbtable", "schema.tablename")
				//.option("user", "admin")
				//.option("password", "admin")
				.load();

		//Encoder<BusJavaBean> busJavaBeanEncoder = Encoders.bean(BusJavaBean.class);
		JavaRDD<BusJavaBean> busRDD = spark.read().textFile("/test/2014bus.txt").javaRDD()
				.map(new Function<String, BusJavaBean>() {
					private static final long serialVersionUID = 8045702091846012650L;

					@Override
						 public BusJavaBean call(String s) {
							String[] parts = s.split(",");

							BusJavaBean bean = new BusJavaBean();
							bean.setCardID(parts[0]);
							bean.setLineID(Integer.valueOf(parts[1]));
							bean.setBeginTime(parts[2]);
							bean.setEndTime(parts[3]);
							bean.setStationFrom(Integer.valueOf(parts[4]));
							bean.setStationTo(Integer.valueOf(parts[5]));
							bean.setBusId(Integer.valueOf(parts[6]));
							return bean;
						 }
					 }
				);


		Dataset<Row> row = spark.createDataFrame(busRDD,BusJavaBean.class);

		row.write().format("jdbc")
				.options(mysqlProperties)
				.mode("append")
				.save();
		//查询结果
		jdbcDF.show(10);

		spark.close();

	}

	public static void jdbcLoad(){

		Map<String,String> mysqlProperties = new HashMap<>();

		mysqlProperties.put("user", "admin");
		mysqlProperties.put("password", "admin");
		mysqlProperties.put("dbtable", "busdata");
		mysqlProperties.put("url", "jdbc:mysql://192.168.42.27:3306/test");

		// Create a JDBC source
		Dataset<Row> jdbcDF = spark.read()
				.format("jdbc")
				.options(mysqlProperties)
				.load();

		//toJavaRDD

		JavaRDD<Row> jdbcRDD = jdbcDF.javaRDD().filter(
				new Function<Row, Boolean>() {
					private static final long serialVersionUID = 4076359785198496746L;

					@Override
					public Boolean call(Row row) {

						if (row.getString(0).equals("70F8CF7C5C507F2516147ADAA7765A80")){
							return true;
						}
						return false;
					}
				}
		);

		System.out.println("RDDSize: " + jdbcRDD.count());

		jdbcRDD.foreach(
				new VoidFunction<Row>() {
					private static final long serialVersionUID = 8218262298739718428L;

					@Override
					public void call(Row row) {
						System.out.println(row.getString(0));
						System.out.println("===================");
						System.out.println(row.mkString("||"));
	/*
	mysql> select * from busdata where cardID='70F8CF7C5C507F2516147ADAA7765A80';
	+----------------------------------+--------+----------------+----------------+-------------+-----------+-------+
	| cardID                           | lineID | beginTime      | endTime        | stationFrom | stationTo | busID |
	+----------------------------------+--------+----------------+----------------+-------------+-----------+-------+
	| 70F8CF7C5C507F2516147ADAA7765A80 |    816 | 20140401173500 | 20140401180949 |          34 |        37 | 17499 |
	+----------------------------------+--------+----------------+----------------+-------------+-----------+-------+
	1 row in set (0.00 sec)
	*/
	/*70F8CF7C5C507F2516147ADAA7765A80
	===================
	70F8CF7C5C507F2516147ADAA7765A80||816||20140401173500||20140401180949||34||37||17499
	这说明Row对象中确实是按数组方式存放的一行所有的数据，
	*/
					}
				}
		);

	}
}

