package spark.spark2upgrade.StructuredStreaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

public class StructuredSocketWordCount {

	public static void main(String[] args) throws StreamingQueryException {

		SparkSession spark = SparkSession.builder()
				.master("local").appName("StructuredSocketWordCount").getOrCreate();

		//创建输入流
		Dataset<Row> lines = spark.readStream()
				.format("socket")
				.option("host","127.0.0.1")
				.option("port","9999")
				.load();

		Dataset<String> words = lines.as(Encoders.STRING())
				.flatMap((FlatMapFunction<String, String>)
						x -> Arrays.asList(x.split(",")).iterator(),Encoders.STRING());

		//words.show();

		StreamingQuery query = words.writeStream()
				.outputMode("complete")//完整打印，还有追加和更新的模式
				.format("console")//打印到控制台"console"
				.start();	//在这里单独调用start方法

		query.awaitTermination();


	}
}
