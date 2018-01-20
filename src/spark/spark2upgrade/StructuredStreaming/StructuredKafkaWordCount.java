package spark.spark2upgrade.StructuredStreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class StructuredKafkaWordCount {
	/**
	 * 与kafka继承，这里只写一个例子，官方上还有一些继承的方式
	 * @param args
	 */
	public static void main(String[] args){

		SparkSession spark = SparkSession.builder()
				.master("local").appName("StructuredKafkaWordCount").getOrCreate();

		Dataset<Row> df =spark.readStream()
				.format("kafka")
				.option("kafka.bootstrap.servers", "192.168.42.24:9092")
				.option("subscribe", "dsf")
				.load();

		df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

		//呵呵...看样子有继承例子但是还不能用，毕竟是beta版
		//Failed to find data source: kafka.
		//Please find packages at http://spark.apache.org/third-party-projects.html

	}
}
