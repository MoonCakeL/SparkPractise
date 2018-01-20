package spark.core;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * mapPartitionsWithIndexDemo算子于mapPartition算子、map算子，本身的功能于一样
 * 但加入我们在元数据生成RDD之前，制定了parallelize集合，spark就会根据实际集合数目
 * 对数据进行分区，假如numPartition是2，那么就会对数据分成2个分区，但具体是那写数据
 * 被分在一个分区内，是不知道的。
 *
 * 但mapPartitionsWithIndexDemo会根据分区数"重新分区（重新分区一般都会造成shuffle操作）"
 * 返回一个实际数据被parallelize，分区后的
 * 基于并行parallelize集合时的indexI，
 */
public class MapPartitionsWithIndexDemo {

	public static void main(String[] args){
		SparkSession spark = SparkSession.builder()
				.appName("MapPartitionsWithIndexDemo").master("local").getOrCreate();

		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		//创建一份数据
		List<Tuple2<String,Integer>> listData = Arrays.asList(
				new Tuple2<>("x1",100),
				new Tuple2<>("x2",90),
				new Tuple2<>("x3",80),
				new Tuple2<>("x4",70)
		);
		//这里进行parallelize为2的并行化分区，下面看看具体那两个数据被分在一个partition中
		JavaRDD<Tuple2<String,Integer>> javaRDD = jsc.parallelize(listData,2);


		//这里需要说明的是"indexID"便是parallelize并行化分区的ID，从0开始
		//同时在方法外还有一个必须的参数"true"，这个参数是必须的true的涵义是，在call方法内保留parallelize并行化分区的ID
		/*JavaRDD<Tuple2<String,Integer>> indexRDD = javaRDD.mapPartitionsWithIndex(
				new Function2<Integer, Iterator<Tuple2<String, Integer>>, Iterator<Tuple2<String, Integer>>>() {
					@Override
					public Iterator<Tuple2<String, Integer>> call(Integer indexID, Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception {
						return null;
					}
				},true
		)*/

		//老样子用lambda来写20180114 19:08
		//这里需要注意的是因为是基于分区内的map，输入和返回的元素仍然是迭代器的方式
		JavaRDD<Tuple2<String,Integer>> indexRDD = javaRDD.mapPartitionsWithIndex(
				(indexID,x) -> {
									List<Tuple2<String,Integer>> list = new ArrayList<>();

									while (x.hasNext()){
										Tuple2<String,Integer> tuple2 = x.next();

										//这里将parallelize并行化分区的ID和tuple2元祖的第一个元素._1拼接
										Tuple2<String,Integer> tuple3 = new Tuple2<>(
												indexID.toString() + "~" + tuple2._1,tuple2._2);

										list.add(tuple3);
									}

									return list.iterator();
				},true//注意这里的"true"是必须的
		);

		indexRDD.foreach( x -> System.out.println("IndexIdName:[" + x._1 + "] value:[" + x._2 + "]") );
		/*
		IndexIdName:[0~x1] value:[100]
		IndexIdName:[0~x2] value:[90]
		IndexIdName:[1~x3] value:[80]
		IndexIdName:[1~x4] value:[70]*/
	}



}
