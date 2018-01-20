package spark.spark2upgrade.functions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TimeFunctions {
	static SparkSession spark = SparkSession.builder()
			.appName("TimeFunctions").master("local").getOrCreate();

	static Dataset<Row> busDF = spark.read().format("csv")
			.option("inferSchema", "true") //推断Schema
			.option("header", "true")
			.load("src/resource/201404bus.csv");


	public static void main(String[] args) throws Exception{

		//Time();
		Timestamp();
	}

	public static void Time(){

		Dataset<Row> toDateDF = busDF.select("BUSID","BEGINTIME","ENDTIME");

		toDateDF.createOrReplaceTempView("busTime");

		/*
		 spark sql的时间函数主要还是基于timestamp的计算
		 如：20140401173500 会处理不了因为是bigint类型的
		 源码中的描述
		 Converts a date/timestamp/string to a value of string in the format specified by the date
		 */
		Dataset<Row> dateFormatDF = spark.sql(
				"select BUSID," +
						"current_date()," +
						"current_timestamp()," +
						"unix_timestamp()," +
						"ENDTIME from busTime");

		dateFormatDF.show();
	}

	public static void Timestamp() throws Exception{

		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		String fileds = "begintime,endtime";
	//1515655130
		List<String> timeData = Arrays.asList(
				"20140401170949,20140401180951",
				"20140401171522,20140401181656",
				"2014-04-01 18:09:19,20140401191923",
				"1515655130,20140401201551"
		);

		List<StructField> structFieldList = new ArrayList<>();
		for (String filedname : fileds.split(",")){
			StructField filed = DataTypes.createStructField(filedname, DataTypes.StringType,true);
			structFieldList.add(filed);
		}

		StructType schema = DataTypes.createStructType(structFieldList);

		JavaRDD<Row> javaRDD = jsc.parallelize(timeData).map(
				new Function<String, Row>() {
					private static final long serialVersionUID = 5839267204306504310L;

					@Override
					public Row call(String s) {
						String[] strings = s.split(",");
						return RowFactory.create((Object[]) strings);
					}
				}
		);

		Dataset<Row> timeDF = spark.createDataFrame(javaRDD,schema);

		timeDF.createOrReplaceTempView("timestampTable");

		Dataset<Row> timestampDF = spark.sql(
				"select " +
						"begintime," +
						"date_format(begintime,'yyyy-MM-dd HH:mm:ss') format1," +
						"date_format(begintime,'yyyy-MM-dd') format2," +
						"endtime " +
						"from timestampTable"
		);
		timestampDF.show();

	}
}
