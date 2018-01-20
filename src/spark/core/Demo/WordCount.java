package spark.core.Common.Demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

public class WordCount implements Serializable {
	static SparkConf conf = new SparkConf().setMaster("local").setAppName("test");
	static JavaSparkContext sc = new JavaSparkContext(conf);
	
	static JavaRDD<String> javaRDD = sc.textFile("D:/eclipse/file/work.txt");
	
	public static void main(String[] args) {
		//WordCount();
		
		WordCountByValue();

	}

	public static void WordCount(){
		
		JavaRDD<String> workRDD = javaRDD.flatMap(
				new FlatMapFunction<String, String>() {
					public Iterator<String> call(String arg){
						
					return Arrays.asList(arg.split(" ")).iterator();
					}
				});
		
		JavaPairRDD<String, Integer> workCount = workRDD.mapToPair(
				new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String arg){
						
						return new Tuple2(arg,1);
						
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer a, Integer b) { return a + b; }
				});	
		//workCount.foreach((x,y) -> (System.out.println(x.toString() + y.)));
		System.out.println(workCount.collect());
	}
	
	
	public static void WordCountByValue(){		
/*		
		JavaRDD<String> workNum = javaRDD.flatMap(
				new FlatMapFunction<String, String>() {
					*//**
					 * 
					 *//*
					private static final long serialVersionUID = 1L;

					public Iterator<String> call(String arg){
						
					return Arrays.asList(arg.split(" ")).iterator();
					}
				});
		JavaPairRDD<String, Integer> workCount = (JavaPairRDD<String, Integer>) workNum.countByValue();*/
		
		Map<String, Long> workCount = javaRDD.flatMap(
				new FlatMapFunction<String, String>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public Iterator<String> call(String arg){
						
					return Arrays.asList(arg.split(" ")).iterator();
					}
				}).countByValue();
		
		//System.out.println(workCount.collect());
		
		System.out.println(workCount.toString());
	}
}
