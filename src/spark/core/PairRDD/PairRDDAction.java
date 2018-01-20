package spark.core.Common.PairRDD;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class PairRDDAction {
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
	
	public static void main(String[] args) {
		
		/*countByKey() 对每个键对应的元素分别计数 返回类型是个Map<String, Long>
		
		collectAsMap() 将结果以映射表的形式返回，以便查询rdd.collectAsMap() Map{(1, 2), (3,4), (3, 6)}
		
		lookup(key) 返回给定键对应的所有值rdd.lookup(3) [4, 6]*/
		//WordCount.WordCountByValue();
		
		//JavaPairRDDcollectAsMap();
		
		JavaPairRDDlookup();
	}
	
	public static void JavaPairRDDcollectAsMap(){
		//{(1=5),2=6,1=8, 3=7} 以map的方法存放和返回(1=5)其实放进去了，但是因为key相同的原因被覆盖掉了
		System.out.println(javaPairRDD.collectAsMap());
	}

	public static void JavaPairRDDlookup(){						
		
		System.out.println(javaPairRDD.lookup(1));
	}
}
