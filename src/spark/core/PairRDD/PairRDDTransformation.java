package spark.core.Common.PairRDD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class PairRDDTransformation {
	static SparkConf conf = new SparkConf().setMaster("local").setAppName("test");
	static JavaSparkContext sc = new JavaSparkContext(conf);

	/*Java 没有自带的二元组类型，因此Spark 的Java API 让用户使用scala.Tuple2 类来创建二
	元组。这个类很简单：Java 用户可以通过new Tuple2(elem1, elem2) 来创建一个新的二元
	组，并且可以通过._1() 和._2() 方法访问其中的元素。*/
	//static Tuple2<Integer,Integer> t1 = new Tuple2<Integer,Integer>(1, 5);
	static Tuple2 a1 = new Tuple2(1, 5);
	static Tuple2 a2 = new Tuple2(2, 6);
	static Tuple2 a3 = new Tuple2(3, 7);
	static Tuple2 a4 = new Tuple2(1, 8);

	static Tuple2 b1 = new Tuple2(9, 5);
	static Tuple2 b2 = new Tuple2(4, 6);
	static Tuple2 b3 = new Tuple2(5, 7);
	static Tuple2 b4 = new Tuple2(1, 8);
	
	
	//static List list = new ArrayList<Tuple2>();
	//list.add(t1);
	//list.add(t2);
	//list.add(t3);
	//list.add(t4);	
	static List list = Arrays.asList(a1,a2,a3,a4);
	static List list2 = Arrays.asList(b1,b2,b3,b4);
	static JavaPairRDD<Integer, Integer> javaPairRDD = sc.parallelizePairs(list);
	static JavaPairRDD<Integer, Integer> javaPairRDD2 = sc.parallelizePairs(list2);
	
		//PairRDD的转换和RDD的常用(转换\“行动？？？”)逻辑基本是一样的意思
	public static void main(String[] args) {
		//“reduce”ByKey 根据具有相同的键进行，对值得进行二元运算
		//PairRDDreduceByKey();
		
		//groupByKey,对具有相同键的值进行分组，所以键的部分是一个List的输出，在转换的PariRDD中需要使用
		//JavaPairRDD<Integer,Iterable<Integer>>,"Iterable"来接收
		//PairRDDgroupByKey();
		//mapValues和flatMapValue的方法同JavaRDD中的map和flatMap，但都是基于值的操作
		//flatMapValue通用是可以输出比原RDD个数多的元素，同样需要使用迭代器输出，map是进来多少个出去多少个
		//PairRDDmapValues();
		//PairRDDflatMapValue();
		
		/*可以直接转化输出，也可以转换成为一个新的PairRDD
		 keys() 返回一个仅包含键的RDD rdd.keys()

		values() 返回一个仅包含值的RDD rdd.values() {

		sortByKey() 返回一个根据键排序的RDD rdd.sortByKey() */
		
		/*PairRDDkeys();
		PairRDDvalues();
		PairRDDsortByKey();*/

		//删掉RDD 中键与other RDD 中的键相同的元素
		//PairRDDsubtractByKey();
		
		//根据RDD中相同的key进行内连接，转换的rdd对象接收应该定义JavaPairRDD<Integer,Tuple2<Integer,Integer>>
		PariRDDjoin();
		
		
		//对两个RDD 进行连接操作，确保第一个RDD 的键必须存在（右外连接），如果第一个RDD的键在第二个中没有
		//那么就显示空Optional.empty
		/*(4,(Optional.empty,6))
		(1,(Optional[5],8))
		(1,(Optional[8],8))
		(9,(Optional.empty,5))
		(5,(Optional.empty,7))*/
		//PariRDDrightOuterJoin();
		
		//对两个RDD 进行连接操作，确保第一个RDD 的键必须存在（左外连接），如果第一个RDD的键在第二个中没有
		//那么就显示空Optional.empty
		/*(1,(5,Optional[8]))
		(1,(8,Optional[8]))
		(3,(7,Optional.empty))
		(2,(6,Optional.empty))*/
		//PariRDDleftOuterJoin();
		
		//将两个RDD 中拥有相同键的数据分组到一起
		/*(4,([],[6]))
		(1,([5, 8],[8]))
		(3,([7],[]))
		(9,([],[5]))
		(5,([],[7]))
		(2,([6],[]))*/
		//PariRDDcogroup();
	}
	
	public static void PairRDDreduceByKey(){
		
		//int sumNum2 = javaRDD1.reduce((v1,v2) -> v1+v2);
		
	/*	javaPairRDD<Integer,Integer> javaPRDD2 = javaPairRDD.reduceByKey(
				
				(a,b) -> a+b)
				
				);*/
		
		JavaPairRDD<Integer,Integer> javaPRDD = javaPairRDD.reduceByKey(
				new Function2<Integer, Integer, Integer>() {
	
					private static final long serialVersionUID = 1L;
					@Override
					public Integer call(Integer arg0, Integer arg1) throws Exception {

						return arg0+arg1;
					}				
		} );
		
		System.out.println(javaPRDD.collect());		
		javaPRDD.foreach(x -> System.out.println(x.toString()));
	}
	
	public static void PairRDDgroupByKey(){
		
		JavaPairRDD<Integer,Iterable<Integer>> javaPRDD = javaPairRDD.groupByKey();
		
		javaPRDD.foreach(x -> System.out.println(x.toString()));
	}
	
	public static void PairRDDmapValues(){
		
		JavaPairRDD<Integer, Integer> javaPRDD = javaPairRDD.mapValues(
				new Function<Integer,Integer>() {
					private static final long serialVersionUID = 1L;
					//只是基于值的计算，所以虽然进来是<Integer,Integer>，
					//public Integer call(Integer arg)但实际值计算一个值和返回一个值map的总数也不会改变
					public Integer call(Integer arg){
						return arg +1;
					}
				});
		javaPRDD.foreach( x -> System.out.println(x.toString()));
	}
	
	public static void PairRDDflatMapValue(){
		
		JavaPairRDD<Integer, Integer> javaPRDD = javaPairRDD.flatMapValues(
				new Function<Integer,Iterable<Integer>>(){
					private static final long serialVersionUID = 1L;

					public Iterable<Integer> call(Integer arg){
						ArrayList list = new ArrayList(); 
						list.add(arg);
						list.set(0, 0);
						return list;
					}
				});
		javaPRDD.foreach( x -> System.out.println(x.toString()));
	}
	
	public static void PairRDDkeys(){
		
		System.out.println("keys() 返回一个仅包含键的RDD" + javaPairRDD.keys().collect());
	}
	
	public static void PairRDDvalues(){	
		System.out.println("values() 返回一个仅包含值的RDD" + javaPairRDD.values().collect());
	}
	
	public static void PairRDDsortByKey(){
		System.out.println("sortByKey() 返回一个根据键排序的RDD" + javaPairRDD.sortByKey().collect());
	}
	
	public static void PairRDDsubtractByKey(){
		
		JavaPairRDD<Integer, Integer> javaPRDD = javaPairRDD.subtractByKey(javaPairRDD2);
		
		javaPRDD.foreach(x -> System.out.println(x.toString()));
	}
	
	public static void PariRDDjoin(){
		JavaPairRDD<Integer,Tuple2<Integer,Integer>> javaPRDD = javaPairRDD.join(javaPairRDD2);
		
		javaPRDD.foreach(x -> System.out.println(x.toString()));
	}
	
	public static void PariRDDrightOuterJoin(){
		JavaPairRDD<Integer,Tuple2<Optional<Integer>,Integer>> javaPRDD 
		= javaPairRDD.rightOuterJoin(javaPairRDD2);
		
		javaPRDD.foreach(x -> System.out.println(x.toString()));
	}
	
	public static void PariRDDleftOuterJoin(){
		JavaPairRDD<Integer, Tuple2<Integer, Optional<Integer>>> javaPRDD 
		= javaPairRDD.leftOuterJoin(javaPairRDD2);
		
		javaPRDD.foreach(x -> System.out.println(x.toString()));
	}
	
	public static void PariRDDcogroup(){
		JavaPairRDD<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> javaPRDD 
		= javaPairRDD.cogroup(javaPairRDD2);
		
		javaPRDD.foreach(x -> System.out.println(x.toString()));
	}
}
