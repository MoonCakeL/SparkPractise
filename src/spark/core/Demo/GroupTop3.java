package spark.core.Demo;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

//对每个班级的分数，进行按照班级分组并显示top3
public class GroupTop3 {
	private static SparkSession spark = SparkSession.builder()
			.appName("groupTop3").master("local").getOrCreate();

	//private static JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

	public static void main(String[] args){
			GroupTop3Demo();
	}

	private static void GroupTop3Demo(){
		//1.先读取数数生成PairRDD
		JavaPairRDD<String,Integer> ClassRDD = spark.read().textFile("src/resource/groupTop3").javaRDD()
		/*sc.textFile("src/resource/groupTop3")*/
				.mapToPair(
						new PairFunction<String, String, Integer>() {
							private static final long serialVersionUID = -1435451110770299745L;

							@Override
							public Tuple2<String, Integer> call(String s) throws Exception {
								String[] line = s.split(" ");
								return new Tuple2<>(line[0],Integer.valueOf(line[1]));
							}
						}
				);
		//2.对原始RDD进行groupbykey，对班级进行分组
		JavaPairRDD<String, Iterable<Integer>> ClassGroupRDD = ClassRDD.groupByKey();

		//3.通过逻辑算法，取出分好组后，对每个班级中分数进行判断，判断出top3的分数
		//通过Iterable迭代器循环遍历判断,返回的PairRDD仍然是JavaPairRDD<String,Iterable<Integer>>的结果

		JavaPairRDD<String,Iterable<Integer>> ClassGroupTop3RDD = ClassGroupRDD.mapToPair(
				new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
					private static final long serialVersionUID = -2462426742007326100L;

					@Override
					public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> s) throws Exception {

						//先定义一个长度为3的数组，用来存储固定数组长度top3和循环遍历数组中分数的大小
						Integer[] top3 = new Integer[3];

						//开始通过迭代器取出每一个分数，却和长度为3的数组中的分数进行循环比较，存储最大的三个分数
						Iterator<Integer> score = s._2.iterator();

						while (score.hasNext()) {

							Integer scores = score.next();

							for (int i = 0; i < top3.length; i++) {

								//开始按照数组长度循环存储分数,按照3的长度进行下标循环，
								//如果数组中下标位置为null就进行赋值操作
								if (top3[i] == null) {
									top3[i] = scores;
									//长度为3的数组，如果装满了，就先进行数组内的数组判断
									//因为score.next是迭代器的缘故，且要比较三个数组
									//所以装满了就开始比较
									break;
									//数组装满后直接break进去else if判断数组中当前存储数字的大小
									//因为是迭代器的原因，每次进来一个值都会for循环3次，进行一个值
									//对数组中3个数字的比较，所以如果当前进来的数组比top3[i]位置上
									//的数字大时，应当移除当前这个top3[i]位置上的数字，把新进来的
									//score.next数字放在这个位置上，如果当前进来的score.next小于
									//top[i]位置上的这个数字则直接跳出，说明当前这个score.next值，比
									//top[i]位置上的数字小，不需要放进数组的这个位置上
								}else if (top3[i] < scores){
									//进行数组内移除当前数组位置的循环
									//和当前数组位置i进行循环三次的位置比较
									//循环开始j=2，j--,当j>i时，说明已经循坏到i的位置，然后进行数组中该位置的移除
									for(int j=2; j>i; j--){
										top3[j] = top3[j-1];
									}
									//通过上面的循环移除当前top3[i]位置的分数后，将score.next放在该位置上
									//完成一次大小的比较，并移除数组小于当前score.next值，并在该位置上
									//重新赋值为score.next的值
									top3[i] = scores;
									//当前这个score.next值，比top[i]位置上的数字小，不需要放进数组的这个位置上
									break;
								}
							}
						}
						return new Tuple2<>(s._1, Arrays.asList(top3));
					}
				}
		);

		ClassGroupTop3RDD.foreach(
				new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
					private static final long serialVersionUID = -2490272585223087148L;

					@Override
					public void call(Tuple2<String, Iterable<Integer>> s) throws Exception {
						System.out.println("班级: " + s._1);

						Iterator<Integer> iterator = s._2.iterator();
						while (iterator.hasNext()){
							System.out.println("score: " + iterator.next());
							System.out.println("==========班级==========");
						}
					}
				}
		);

	}

}
