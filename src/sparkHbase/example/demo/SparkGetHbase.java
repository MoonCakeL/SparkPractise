package sparkHbase.example.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkGetHbase {

	public static void main(String[] args){
		String tableName = args[0];

		SparkConf sparkConf = new SparkConf()
				.setAppName("JavaHBaseBulkGetExample " + tableName)
				.setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
	}
}
