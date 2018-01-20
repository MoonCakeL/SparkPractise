package spark.core.Common.Demo;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;

public class LineCount {
	static SparkSession spark = SparkSession.builder()
			.appName("LineCount")
			.master("local")
			.getOrCreate();

	static JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
	public static void main(String[] args){
		LineCount();

	}

	public static void LineCount(){

		JavaPairRDD<String,Integer> lineCountRDD = sc.textFile("src/resource/lineCount.txt")/*.flatMap(
				new FlatMapFunction<String, String>() {
					@Override
					public Iterator<String> call(String s) throws Exception {

						return Arrays.asList(s.split("/n")).iterator();
					}
				}
		)*/.mapToPair(
				new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = -1630200753770431092L;

					@Override
					public Tuple2<String, Integer> call(String s) throws Exception {
						return new Tuple2<String,Integer>(s,1);
					}
				}
		).reduceByKey(
				new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = -1201202002526159273L;

					@Override
					public Integer call(Integer integer, Integer integer2) throws Exception {
						return integer + integer2;
					}
				}
		);

		List<Tuple2<String,Integer>> listLineCount = lineCountRDD.collect();

		sc.close();
		for (Tuple2<String,Integer> tuple2 : listLineCount){
			System.out.println("Line: " + tuple2._1 + " Count: " + tuple2._2);
		}
	}
}
