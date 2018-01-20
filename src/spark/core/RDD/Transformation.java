package spark.core.Common.RDD;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;


public class Transformation {
	static SparkConf conf = new SparkConf().setMaster("local").setAppName("test");
	static JavaSparkContext sc = new JavaSparkContext(conf);
	
	public static void main(String[] args){
		//JavaRDD.map将数据集内的每一个元素经过一个function（处理）。然后将处理结果返回到一个新JavaRDD中
		//JavaRDDMap();
		
		//JavaRDD.flastMap 将数据集内的每一个元素经过function（处理之后会变多的情况下使用），会返回比之前
		//个数要多的元素，所以需要使用到iterator迭代输出每一个新的元素到一个新JavaRDD中
		//JavaRDDflatMap();
				
		//***以下四种属于RDD本身的伪集合操作，支持一些数学操作，但要求两个RDD中的元素必须为同数据类型
		//1 distinct()操作的开销很大，因为它需要将所有数据通过网络进行混洗（shuffle），以确保每一个元素只有一份
		//JavaRDDdistinct();
		
		//2 intersection()交集，接收两个RDD，通过第一个在第二个中进行比较，性能较差，需要网络混洗发现共有元素
		//JavaRDDintersection();
		
		//3 subtract(),接收两个RDD，通过比较扣除第一个RDD在第二个RDD中共有的元素，返回不存在在第二个RDD中的元素，
		//同intersection()一样性能较差，需要网络混洗
		//JavaRDDsubtract();
		
		//4 union合并，接收两个RDD，不会去重，也不需要在网络中进行混洗
		//JavaRDDunion();
		
		//cartesian()求大规模RDD 的笛卡儿积开销巨大
		JavaRDDcartesian();
	}
	
	public static void JavaRDDMap() {
		/*List<Integer> l = new ArrayList<Integer>();
        List<List<Integer>> k = new ArrayList<List<Integer>>();
        for(int i = 0;i<=10;i++){
        	
            l.add(i);
            
        }

        k.add(l);*/
		
		JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10));
		//java写真的是很麻烦一个map+1的rdd需要这么多行，在scala中直接javaRDD.map(x => x+1);就可以了
		//JavaRDD.map将数据集内的每一个元素经过一个function（处理）。然后将处理结果返回到一个新JavaRDD中
		System.out.println(javaRDD.collect());
		JavaRDD<Object> javaRDD2 = javaRDD.map(
				f ->Arrays.asList(f.toString() + ":" + " toString") 
				//新写法 很简洁
				//f ->Arrays.asList(f + 1)
				/*
				//代表进来一个Integer，出去一个string
				new Function<Integer, String>()
				 {
					private static final long serialVersionUID = 1L;
					//call遍历内部内容,返回字符串这里，可以当做一个小方法，也可以是void
					public String call(Integer javaRDD) {
						return (javaRDD.toString() + ":" + " toString");
					}
				 }
			*/);
		System.out.println(javaRDD2.collect());
	}
	
	public static void JavaRDDflatMap(){
		JavaRDD<String> javaRDD = sc.parallelize(Arrays.asList("a,a","b,b","c,c","d,d"));
		
		JavaRDD<String> javaRDD2 = javaRDD.flatMap(
				//新写法 很简洁 jdk8提供的新写法
				f -> Arrays.asList(f.split(",")).iterator()
				/*
				new FlatMapFunction<String, String>() {
					private static final long serialVersionUID = 1L;

					public Iterator<String> call(String javaRDD){
						return (Iterator<String>) Arrays.asList(javaRDD.split(","));
					}
				}
				*/);
		System.out.println(javaRDD2.collect());
		//StringUtils.join
		System.out.println(StringUtils.join(javaRDD2.collect(),"|"));
	}
	
	public static void JavaRDDdistinct(){
		JavaRDD<String> javaRDD = sc.parallelize(Arrays.asList("a","b","a","d","b"));
		
		JavaRDD<String> javaRDD2 = javaRDD.distinct();
		
		System.out.println(javaRDD2.collect());
	}
	
	public static void JavaRDDintersection(){
		JavaRDD<String> javaRDD1 = sc.parallelize(Arrays.asList("a","b","c","d","e"));
		JavaRDD<String> javaRDD2 = sc.parallelize(Arrays.asList("a","b","c"));
		
		JavaRDD<String> javaRDD = javaRDD1.intersection(javaRDD2);
		
		System.out.println( StringUtils.join(javaRDD.collect(),"|") );
	}
	
	public static void JavaRDDunion(){
		JavaRDD<String> javaRDD1 = sc.parallelize(Arrays.asList("a","b","c","d","e"));
		JavaRDD<String> javaRDD2 = sc.parallelize(Arrays.asList("a","b","c"));
		
		JavaRDD<String> javaRDD = javaRDD1.union(javaRDD2);
		
		System.out.println( StringUtils.join(javaRDD.collect(),"|") );
	}
	
	public static void JavaRDDsubtract(){
		JavaRDD<String> javaRDD1 = sc.parallelize(Arrays.asList("a","b","c","d","e"));
		JavaRDD<String> javaRDD2 = sc.parallelize(Arrays.asList("a","b","c"));
		
		JavaRDD<String> javaRDD = javaRDD1.subtract(javaRDD2);
		
		System.out.println( StringUtils.join(javaRDD.collect(),"|") );
	}
	
	public static void JavaRDDcartesian(){
		JavaRDD<String> javaRDD1 = sc.parallelize(Arrays.asList("a","b","c"));
		JavaRDD<Integer> javaRDD2 = sc.parallelize(Arrays.asList(1,2,3));
		
		//求笛卡尔积，返回的是两个RDD的笛卡尔积，有分区的概念需要用到JavaPairRDD
		JavaPairRDD<String,Integer> javaRDD = javaRDD1.cartesian(javaRDD2);
		
		System.out.println( StringUtils.join(javaRDD.collect(),"|") );
		//(a,1)|(a,2)|(a,3)|(b,1)|(b,2)|(b,3)|(c,1)|(c,2)|(c,3)
	}
}
