package spark.core.Common.PairRDD;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class CreatePairRDD {
	public static SparkConf conf = new SparkConf()
								.setMaster("local")
								.setAppName("test");
	
	public static JavaSparkContext sc = new JavaSparkContext(conf);
	
	public static void main(String[] args) {
		
		JavaRDD<String> javaRDD = sc.parallelize(Arrays.asList("1 aa","2 bb","3 cc","4 dd"));
		
		//通过空格分隔前一个字符作为key，Java 用户还需要调用专门的Spark 函数来创建pair RDD。
		//例如，要使用mapToPair() 函数来代替基础版的map() 函数
		//JavaPairRDD 键值 对应的 RDD数据集类型
		
		/*JavaPairRDD<String, String> pairRDD2 = javaRDD.mapToPair
				//lines.map(x => (x.split(" ")(0), x))
				(x -> (x.split(" ")[0],x));*/
		
		/*Java 没有自带的二元组类型，因此Spark 的Java API 让用户使用scala.Tuple2 类来创建二
		元组。这个类很简单：Java 用户可以通过new Tuple2(elem1, elem2) 来创建一个新的二元
		组，并且可以通过._1() 和._2() 方法访问其中的元素。*/
		
		JavaPairRDD<String, String> pairRDD = javaRDD.mapToPair(
				new PairFunction<String, String, String>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String,String> call(String x){
						return new Tuple2(x.split(" ")[0],x);
					}
				});

		System.out.println("lambda:-----------");
		pairRDD.foreach(x -> System.out.println(x.toString()));
		
		pairRDD.foreach(new VoidFunction<Tuple2<String,String>>() {

			@Override
			public void call(Tuple2<String, String> arg0) throws Exception {
				System.out.println("arg0:-----------");
				System.out.println(arg0);
				
			}
		});


		System.out.println(pairRDD.collect());
	}
	
	

}
