package spark.SparkStreaming.kafkaDirectOffsetCommit;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class TakeInKafkaDirectWordCount {

	public static void KafkaDirectWordCount(JavaRDD<String> javaRDD){

		JavaPairRDD<String,Integer> wordCount = javaRDD.flatMap(
				new FlatMapFunction<String, String>() {
					private static final long serialVersionUID = 7109433319243137921L;

					@Override
					public Iterator<String> call(String x) {
						return Arrays.asList(x.split(",")).iterator();
					}
				}
		).mapToPair(
				new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 5431530959239205489L;

					@Override
					public Tuple2<String, Integer> call(String x) {
						return new Tuple2<>(x, 1);
					}
				}
		).reduceByKey(
				new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = -8748598679734081474L;

					@Override
					public Integer call(Integer integer, Integer integer2) {
						return integer + integer2;
					}
				}
		);
		wordCount.foreach(
				new VoidFunction<Tuple2<String, Integer>>() {
					private static final long serialVersionUID = -3253465851730875898L;

					@Override
					public void call(Tuple2<String, Integer> x) {
						System.out.println(
								" keyword :[" + x._1 + "]  " +
										"count : [" + x._2 + "]");
					}
				}
		);

	}
}
