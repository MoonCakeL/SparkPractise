package spark.sparkSQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class DataFrameCreateHDFS {
	public static void main(String[] args){

		System.setProperty("HADOOP_USER_NAME", "hadoop");
		SparkSession spark = SparkSession.builder()
				.appName("sqltest").master("local").getOrCreate();

		Dataset<String> dataFrame = spark.read()
				.textFile("hdfs://192.168.42.24:9000/test/wordcounttest2.txt");

		dataFrame.printSchema();
		dataFrame.show();
	}
}
