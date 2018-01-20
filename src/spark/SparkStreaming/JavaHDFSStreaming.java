package spark.SparkStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class JavaHDFSStreaming {
	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf()
				.setAppName("JavaHDFSStreaming").setMaster("local[2]");

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

		JavaDStream<String> dStream = jssc.textFileStream("hdfs:hadoop1:9000/yangyu").flatMap(
				new FlatMapFunction<String, String>() {
					private static final long serialVersionUID = -6103817777573681070L;

					@Override
					public Iterator<String> call(String s) {
						return Arrays.asList(s.split(",")).iterator();
					}
				}
		);

		JavaPairDStream<String,Integer> wordCount = dStream.mapToPair(
				new PairFunction<String, String, Integer>() {

					private static final long serialVersionUID = -3193325761007638710L;

					@Override
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<>(s,1);
					}
				}
		).reduceByKey(
				new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 2338886753588368349L;

					@Override
					public Integer call(Integer integer, Integer integer2) {
						return integer + integer2;
					}
				}
		);

		//Aciton操作，如果没有Action操作，前面的转换操作会报错
		wordCount.print();

		jssc.start();
		jssc.awaitTermination();

		jssc.stop();
		//jssc.close();
	}
}
