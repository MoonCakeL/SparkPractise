package spark.core.Common;

import jodd.util.StringUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Transformation {
	static SparkSession spark = SparkSession.builder()
			.master("local")
			.appName("TransformationDemo")
			.getOrCreate();

	static JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

	/**
	 * 1.map:将集合内每个元素加2
	 * 2.filter：过滤出集合中的偶数
	 * 3.flatMap：将行拆分成单词
	 * 4.groupByKey：将每个班级的成绩进行分组
	 * 5.reduceByKey:统计每个班级的总分
	 * 6.sortByKey：将学生分数进行排序
	 * 7.join：打印每个学生的成绩
	 * 8.cogroup：打印每个学生的成绩
	 * @param args
	 */
	public static void main(String[] args){
			//mapRDD();
			//filterRDD();
			//flatMapRDD();
			//groupByKeyRDD();
			//reduceByKeyRDD();
			//sortByKeyRDD();
			joinRDD();
			cogroupRD();

	}

	public static void mapRDD(){
		JavaRDD<Integer> mapRDD = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10)).map(
				new Function<Integer, Integer>() {
					@Override
					public Integer call(Integer integer) throws Exception {
						return integer+2;
					}
				}
		);
		List<Integer> lisMapRDD = mapRDD.collect();

		sc.close();
		for (Integer list : lisMapRDD){
			System.out.println(list);
		}
	}

	public static void filterRDD(){
		JavaRDD<Integer> filterRDD = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10)).filter(
				new Function<Integer, Boolean>() {
					@Override
					public Boolean call(Integer integer) throws Exception {
						return integer%2==0;
					}
				}
		);
		List<Integer> lisMapRDD = filterRDD.collect();

		sc.close();
		for (Integer list : lisMapRDD){
			System.out.println(list);
		}
	}

	public static void flatMapRDD() {
		JavaRDD<String> flatMapRDD = sc.parallelize(Arrays.asList("a b c d e f")).flatMap(
				new FlatMapFunction<String, String>() {
					@Override
					public Iterator<String> call(String s) throws Exception {
						return Arrays.asList(s.split(" ")).iterator();
					}
				}
		);
		List<String> lisMapRDD = flatMapRDD.collect();

		sc.close();
		for (String list : lisMapRDD) {
			System.out.println(list);
		}
	}

		//groupByKey返回的还是JavaPairRDD
		//但是，JavaPairRDD的第一个泛型类型不变，第二个泛型类型变成了Iterable这种集合类型
		//说明，按照Key进行分组，那么每个key可能都会有多个value，此时第二个泛型类型就变成了Iterable
		//这种方式可以很方便的处理某个分组内的数据，可以对第二个泛型类型变成了Iterable的多个value进行function
	public static void groupByKeyRDD(){
			List<Tuple2<String,Integer>> list = Arrays.asList(
					new Tuple2<String,Integer>("班级1",100),
					new Tuple2<String,Integer>("班级1",100),
					new Tuple2<String,Integer>("班级2",99),
					new Tuple2<String,Integer>("班级3",90),
					new Tuple2<String,Integer>("班级4",98),
					new Tuple2<String,Integer>("班级1",90),
					new Tuple2<String,Integer>("班级2",100),
					new Tuple2<String,Integer>("班级3",90),
					new Tuple2<String,Integer>("班级4", 80)
			);

		//创建并行化集合，parallelizePairs（）
		JavaPairRDD< String, Iterable<Integer>> groupByKeyRDD = sc.parallelizePairs(list).groupByKey();
		//[(班级2,[99, 100]), (班级1,[100, 100, 90]), (班级3,[90, 90]), (班级4,[98, 80])]
		//System.out.println(groupByKeyRDD.collect());

		groupByKeyRDD.foreach(
				new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
					@Override
					public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
						System.out.println("班级： " + stringIterableTuple2._1);

						Iterator<Integer> iterable = stringIterableTuple2._2.iterator();

						while (iterable.hasNext()){
							System.out.println("分数: " + iterable.next());
						}
						System.out.println("==========班级==========");
					}
				}
		);

		sc.close();
	}

	public static void reduceByKeyRDD(){
		List<Tuple2<String,Integer>> list = Arrays.asList(
				new Tuple2<String,Integer>("班级1",100),
				new Tuple2<String,Integer>("班级1",100),
				new Tuple2<String,Integer>("班级2",99),
				new Tuple2<String,Integer>("班级3",90),
				new Tuple2<String,Integer>("班级4",98),
				new Tuple2<String,Integer>("班级1",90),
				new Tuple2<String,Integer>("班级2",100),
				new Tuple2<String,Integer>("班级3",90),
				new Tuple2<String,Integer>("班级4", 80)
		);

		//创建并行化集合，parallelizePairs（）
		JavaPairRDD< String, Integer> reduceByKeyRDD = sc.parallelizePairs(list).reduceByKey(
				new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer integer, Integer integer2) throws Exception {
						return integer + integer2;
					}
				}
		);

		System.out.println(reduceByKeyRDD.collect());
		sc.close();
	}

	public static void  sortByKeyRDD(){
		List<Tuple2<Integer,String>> list = Arrays.asList(
				new Tuple2<Integer,String>(12,"小1"),
				new Tuple2<Integer,String>(33,"小4"),
				new Tuple2<Integer,String>(43,"小2"),
				new Tuple2<Integer,String>(2,"小7"),
				new Tuple2<Integer,String>(78,"小9"),
				new Tuple2<Integer,String>(8,"小0")
		);

		JavaPairRDD<Integer,String> sortByKeyRDD = sc.parallelizePairs(list).sortByKey(false); //.sortByKey()升序.sortByKey(false)降序

		System.out.println(sortByKeyRDD.collect());
	}

	public static void joinRDD(){
		List<Tuple2<String,Integer>> list = Arrays.asList(
				new Tuple2<String,Integer>("小1",100),
				new Tuple2<String,Integer>("小1",100),
				new Tuple2<String,Integer>("小2",99),
				new Tuple2<String,Integer>("小3",90),
				new Tuple2<String,Integer>("小4",98),
				new Tuple2<String,Integer>("小1",90),
				new Tuple2<String,Integer>("小2",100),
				new Tuple2<String,Integer>("小3",90),
				new Tuple2<String,Integer>("小4", 80)
		);

		List<Tuple2<String,Integer>> list2 = Arrays.asList(
				new Tuple2<String,Integer>("小1",100),
				new Tuple2<String,Integer>("小3",90),
				new Tuple2<String,Integer>("小4", 80)
		);

		JavaPairRDD<String,Integer> joinRDD = sc.parallelizePairs(list);

		JavaPairRDD<String,Integer> joinRDD2 = sc.parallelizePairs(list2);

		System.out.println(StringUtil.join(joinRDD.join(joinRDD2).collect(),"|"));
	}

	public static void cogroupRD(){
		List<Tuple2<String,Integer>> list = Arrays.asList(
				new Tuple2<String,Integer>("小1",100),
				new Tuple2<String,Integer>("小1",100),
				new Tuple2<String,Integer>("小2",99),
				new Tuple2<String,Integer>("小3",90),
				new Tuple2<String,Integer>("小4",98),
				new Tuple2<String,Integer>("小1",90),
				new Tuple2<String,Integer>("小2",100),
				new Tuple2<String,Integer>("小3",90),
				new Tuple2<String,Integer>("小4", 80),
				new Tuple2<String,Integer>("小1",100),
				new Tuple2<String,Integer>("小3",90),
				new Tuple2<String,Integer>("小4", 80)
		);

		List<Tuple2<String,Integer>> list2 = Arrays.asList(
				new Tuple2<String,Integer>("小1",100),
				new Tuple2<String,Integer>("小3",90),
				new Tuple2<String,Integer>("小4", 80)
		);

		JavaPairRDD<String,Integer> joinRDD = sc.parallelizePairs(list);

		JavaPairRDD<String,Integer> joinRDD2 = sc.parallelizePairs(list2);

		System.out.println(StringUtil.join(joinRDD.cogroup(joinRDD2).collect(),"|"));
	}
}
