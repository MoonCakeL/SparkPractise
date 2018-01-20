package spark.core.Demo;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class SortWordCount {

	public static void main(String[] args){
		SparkSession  spark = SparkSession
				.builder().appName("SortWordCount").master("local").getOrCreate();

		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaPairRDD<String,Integer> sortWordCountRDD=
				sc.textFile("src/resource/work.txt").flatMap(
						new FlatMapFunction<String, String>() {
							@Override
							public Iterator<String> call(String s) throws Exception {

								return Arrays.asList(s.split(" ")).iterator();
							}
						}
				).mapToPair(
						new PairFunction<String, String, Integer>() {
							@Override
							public Tuple2<String, Integer> call(String s) throws Exception {
								return new Tuple2<>(s,1);
							}
						}
				).reduceByKey(
						new Function2<Integer, Integer, Integer>() {
							@Override
							public Integer call(Integer integer, Integer integer2) throws Exception {
								return integer + integer2;
							}
						}
				).mapToPair(
						//将原始Tuple2进行反转
						new PairFunction<Tuple2<String, Integer>, Integer,String >() {
							@Override
							public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
								return new Tuple2<>(tuple2._2,tuple2._1);
							}
						}
						//倒序排序完成后再进行反转
				).sortByKey(false).mapToPair(
						//前面是进来的，后面是出去的
						new PairFunction<Tuple2<Integer, String>, String,Integer>() {
							@Override
							//后面是进来的，前面是出去的
							public Tuple2<String,Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
								return new Tuple2<>(tuple2._2,tuple2._1);
							}
						}
				);

		sortWordCountRDD.foreach(
				new VoidFunction<Tuple2<String,Integer>>() {
					@Override
					public void call(Tuple2<String, Integer> tuple2) throws Exception {
						System.out.println("Word : [" + tuple2._1 + "] Count : [" + tuple2._2 + "]");
					}
				}
		);
		sc.close();
	}
}
