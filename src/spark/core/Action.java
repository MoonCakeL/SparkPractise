package spark.core.Common;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Action {
	static SparkSession spark = SparkSession.
			builder()
			.master("local")
			.appName("ActionDeom")
			.getOrCreate();

	static JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

	public static void main(String[] args){
		//reduceRDD();
		//collectRDD();
		//count方法统计RDD中有多少个元素，实际中基本用的很少。
		//saveAsTextFileRDD();
		countByKeyRDD();
		//foreach(func) 对RDD 中的每个元素使用给定的函数 |rdd.foreach(func)
		//实际中最常用的手段，对RDD中每一个元素进行处理并且它是在集群上进行处理不会和collect是拉取回本地，性能很快
	}

	public static void reduceRDD(){

		//Action 操作最终是将RDD行动，执行起来，所以不是在转换一个新的RDD出来，这里使用Interger变量来接受
		Integer reduceRDD = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9)).reduce(
				new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer integer, Integer integer2) throws Exception {
						return integer + integer2;
					}
				}
		);

		System.out.println(reduceRDD);
		sc.close();

	}

	public static void collectRDD(){

		//collect 就是将远程集群上的RDD中的内容以一个List类型拉取到本地然后返回
		//这种方式一般不建议使用，因为要从远程走大量的远程传输，拉取数据到本地，性能差
		//还可能在RDD数据量特别大的时候出现oom异常，内存溢出
		//所以一般还是使用forearch对RDD进行"遍历"输出

		JavaRDD<Integer> collectRDD = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9)).map(
				new Function<Integer, Integer>() {
					@Override
					public Integer call(Integer integer) throws Exception {
						return integer * 2;
					}
				}
		);

		List<Integer> list = collectRDD.collect();

		/*Iterator<Integer> iterator = list.iterator();

		while (iterator.hasNext()){
			System.out.println(iterator.next());
		}*/

		for(Integer integer : list){
			System.out.println(integer);
		}

		sc.close();

	}

	public static void saveAsTextFileRDD(){

		JavaRDD<Integer> collectRDD = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9)).map(
				new Function<Integer, Integer>() {
					@Override
					public Integer call(Integer integer) throws Exception {
						return integer * 2;
					}
				}
		);
		//这里是一个路径，不是一个路径下可以自定义文件的，最终会按照HDFS文件的存储格式进行存储
		//所以完全可以保存在HDFS上"hdfs://hadoop1:9000/saveRDDTest"
		collectRDD.saveAsTextFile("src/resource/saveRDDTest");

		sc.close();

	}

	public static void countByKeyRDD(){
		List<Tuple2<String,Integer>> list = Arrays.asList(
				new Tuple2<String,Integer>("班级1",100),
				new Tuple2<String,Integer>("班级2",100),
				new Tuple2<String,Integer>("班级2",100),
				new Tuple2<String,Integer>("班级1",100),
				new Tuple2<String,Integer>("班级2",100),
				new Tuple2<String,Integer>("班级1",100),
				new Tuple2<String,Integer>("班级2",100),
				new Tuple2<String,Integer>("班级1",100),
				new Tuple2<String,Integer>("班级1",100)
		);

		Map<String,Long> countByKeyRDD = sc.parallelizePairs(list,2).countByKey();

		for (Map.Entry entry : countByKeyRDD.entrySet()){
			System.out.println("Key: [" + entry.getKey() + "]" + " Value: [" + entry.getValue() + "]");
		}

		sc.close();
	}
}
