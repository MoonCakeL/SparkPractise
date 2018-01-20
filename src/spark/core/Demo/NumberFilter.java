package spark.core.Demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class NumberFilter {
	static SparkConf conf = new SparkConf().setMaster("local").setAppName("test");
	static JavaSparkContext sc = new JavaSparkContext(conf);
	
	static Tuple2 a1 = new Tuple2(1, "aaa");
	static Tuple2 a2 = new Tuple2(2, "b");
	static Tuple2 a3 = new Tuple2(3, "ccccc");
	static Tuple2 a4 = new Tuple2(1, "ddddddd");
	
	static List list = Arrays.asList(a1,a2,a3,a4);
	static JavaPairRDD<Integer, String> javaPairRDD = sc.parallelizePairs(list);
	
	public static void main(String[] args) {
		NumberFilterRDD();

	}
			//Pair RDD 也还是RDD（元素为Java 或Scala 中的Tuple2 对象或Python 中的元组），因此
			//同样支持RDD 所支持的函数。
	public static void NumberFilterRDD(){
				
		JavaPairRDD<Integer, String> numberPairRDD1 = javaPairRDD.filter(
				//Tuple2<Integer,String> 通过这样的方式可以变成一个对象来使用RDD的Function方法
			new Function<Tuple2<Integer,String>,Boolean>() {

				private static final long serialVersionUID = 1L;

				public Boolean call(Tuple2<Integer,String> arg0) throws Exception {					
					//arg0._2()基于Tuple2<Integer,String>，可选过滤第一个还是第二个
					
					return arg0._2().length() >=3;
					
				}
			});
		
		//numberPairRDD1.foreach(x -> System.out.println(x.toString()));
		
		
		JavaPairRDD<Integer, String> numberPairRDD2 = javaPairRDD.mapValues(
				new Function<String,String>() {
					public String call(String arg){

						return arg + "111";
					}					
				});
		
		JavaPairRDD<Integer, String> numberPairRDD3 = javaPairRDD.flatMapValues(
				new Function<String, Iterable<String>>() {
					@Override
					public Iterable<String> call(String s) throws Exception {
						List<String> list = Arrays.asList(s.split(""));

						return list;
					}
				}
		);

		List<Tuple2<Integer,String>> list = numberPairRDD3.collect();

		for (Tuple2<Integer,String> tuple2 : list){
			System.out.println("Key: [" + tuple2._1 + "] Value: [" + tuple2._2 + "]");
		}
	}
}
