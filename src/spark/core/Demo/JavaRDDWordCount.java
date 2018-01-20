package spark.core.Common.Demo;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class JavaRDDWordCount {
	static SparkSession spark = SparkSession.builder().appName("JavaRDDWordCount").master("local").getOrCreate();
	static JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

	public static void main(String[] args){
			JavaRDDWordCount();
	}

	public static void JavaRDDWordCount(){
		JavaRDD<String> javaRDD = sc.textFile("src/resource/word.txt");

		JavaPairRDD<String,Integer> RDDWordCount = javaRDD.flatMap(
				new FlatMapFunction<String, String>() {
					@Override
					public Iterator<String> call(String s) throws Exception {

						return Arrays.asList(s.split(" ")).iterator();
					}
				}
		).mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) throws Exception {
				return new Tuple2<>(s,1) ;
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer integer, Integer integer2) throws Exception {
				return integer+integer2;
			}
		});

		List<Tuple2<String,Integer>> listWordCount = RDDWordCount.collect();

		for (Tuple2<String,Integer> tuple2 : listWordCount ){

			System.out.println("word: " + tuple2._1 + " count: " + tuple2._2);
		}
		sc.close();
	}
}
