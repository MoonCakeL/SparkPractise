package spark.core.Common.RDD;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class Action {
	
	public static void main(String[] ages){
			//Action(行动)属于对JavaRDD数据集的“消费”动作，不会产生新的RDD
			//javaRDDreduce(); 对JavaRDD中的每个元素进行二元运算
			//javaRDDcollect();输出全部元素，在RDD元素比较少的时候可以使用
			//javaRDDcount(); count
			//javaRDDcountByValue(); 根据元素出现的个数进行统计groupby value
			//javaRDDtake(); 从RDD 中返回num 个元素，有点想List的下标值，但take是从1开始
			//javaRDDtop();
			javaRDDtakeOrdered();
		
			//对一个数据为{1, 2, 3, 3}的RDD
			/*top(num) 从RDD 中返回最前面(大小)的num个元素  |rdd.top(2) |{3, 3}
			 * 
			takeOrdered(num) 从RDD 中按照提供的顺序返 回最前面的num 个元素 |rdd.takeOrdered(2)(myOrdering)| {3, 3}
			
			takeSample(withReplace ment, num, [seed]) 从RDD 中返回任意一些元素rdd.takeSample(false, 1) | 非确定的
			
			reduce(func) 并行整合RDD 中所有数据 例如sum） |rdd.reduce((x, y) => x + y) |9
			
			fold(zero)(func) 和reduce() 一样， 但是需要提供初始值|rdd.fold(0)((x, y) => x + y)|9
			
			foreach(func) 对RDD 中的每个元素使用给定的函数 |rdd.foreach(func)
*/
		
	}
	
	static SparkConf conf = new SparkConf().setMaster("local").setAppName("test");
	static JavaSparkContext sc = new JavaSparkContext(conf);

	public static void javaRDDreduce(){
		JavaRDD<Integer> javaRDD1 = sc.parallelize(Arrays.asList(1,2,3,4));
		
		//jdk8提供的lambda简写发
		int sumNum2 = javaRDD1.reduce((v1,v2) -> v1+v2);
		
		int sumNum = javaRDD1.reduce(
				new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

				public Integer call(Integer v1,Integer v2) throws Exception{
					return v1 + v2;
				}	
			});
		System.out.println(sumNum);
		System.out.println(sumNum2);
	}
	
	public static void javaRDDcollect(){
		JavaRDD<Integer> javaRDD1 = sc.parallelize(Arrays.asList(1,2,3,4));
				
		System.out.println(javaRDD1.collect());	
		
		JavaRDD<String> javaRDD = javaRDD1.map(x -> x.toString());
		
		System.out.println(javaRDD.collect());
	}
	
	public static void javaRDDcount(){
		JavaRDD<Integer> javaRDD1 = sc.parallelize(Arrays.asList(1,2,3,4));
				
		System.out.println(javaRDD1.count());	
	}
	
	public static void javaRDDcountByValue(){
		JavaRDD<Integer> javaRDD1 = sc.parallelize(Arrays.asList(1,2,3,4,3,4));
				
		System.out.println(javaRDD1.countByValue());	
	}
	
	public static void javaRDDtake(){
		JavaRDD<Integer> javaRDD1 = sc.parallelize(Arrays.asList(1,2,3,4,3,4));
				
		System.out.println(javaRDD1.take(2));	
	}
	
	public static void javaRDDtop(){
		JavaRDD<Integer> javaRDD1 = sc.parallelize(Arrays.asList(1,2,3,4,3,4,10));
				
		System.out.println(javaRDD1.top(2) + "top");	
	}
	
	public static void javaRDDtakeOrdered(){
		JavaRDD<Integer> javaRDD1 = sc.parallelize(Arrays.asList(1,2,3,4,3,4,10));
				
		System.out.println(javaRDD1.takeOrdered(2));
	}
}
