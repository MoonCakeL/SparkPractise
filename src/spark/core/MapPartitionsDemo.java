package spark.core;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * mapPartition算子，本身的功能于map一样，
 * 基于javaRDD的*partition的算子,原因是因为foreach和map这样的算子
 * 内部的实现都是一个一个进行的，如果在内部使用过程中需要频繁创建新对象（new）
 * 那么首先不会那么高效而且对资源造成一定程度的浪费。
 * 而基于*partition的算子是将内部rdd中的每一个元素变成了rdd中每一个分区的迭代器
 * 可以基于一个内部元素分区创建创建一个新对象，让分区内所有元素都可以共享通过一个对象
 * 以便与提升性能，典型的就如同对mysql入库这样的，需要插入一条就创建一个jdbc，开销会比较大。
 * 常用的算子中主要使用比较多的就是：mapPartitions算子和foreachPartition算子
 * ，当然还有几个也是基于partition的但是不常用，详细可以看javardd的源码class中的算子函数（在收藏中）
 *
 * 注意：当然在基于一个分区数据量可能非常大的情况下，需要考虑是否使用，本身的用处可能还是为了介绍map计算
 * 		过程中，如果有需要new新资源时，对new新资源的提升利用率吧
 */

public class MapPartitionsDemo {

	public static void main(String[] args){
		SparkSession spark = SparkSession.builder()
				.appName("MapPartitionsDemo").master("local").getOrCreate();

		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		//创建一份数据
		List<Tuple2<String,Integer>> listData = Arrays.asList(
				new Tuple2<>("x1",100),
				new Tuple2<>("x2",90),
				new Tuple2<>("x3",80),
				new Tuple2<>("x4",70)
		);

		JavaRDD<Tuple2<String,Integer>> javaRDD = jsc.parallelize(listData);


		JavaRDD<Tuple2<String,Integer>> mapRDD = javaRDD.mapPartitions(

				x -> {
					List<Tuple2<String,Integer>> list = new ArrayList<>();
						while (x.hasNext()){

							Tuple2<String,Integer> tuple2 = x.next();
							Tuple2<String,Integer> tuple3 = new Tuple2<>(tuple2._1(),tuple2._2+100);
							list.add(tuple3);
						}
						return list.iterator();
				}

				//这里需要注意的是mapPartitions是基于一个分区内的所有元素进行计算，
				// 所以程序内部是先按照迭代器输入，然后在进行迭代返回，所以类型是个Iterator<T>
				/*new FlatMapFunction<Iterator<Tuple2<String, Integer>>, Tuple2<String,Integer>>() {
					@Override
					public Iterator<Tuple2<String,Integer>> call(Iterator<Tuple2<String, Integer>> x) throws Exception {

							List<Tuple2<String,Integer>> list = new ArrayList<>();

										while (x.hasNext()){
									Tuple2<String,Integer> tuple2 = new Tuple2<>(x.next()._1,x.next()._2+100);
											list.add(tuple2);
										}

						return list.iterator();
					}
				}*/
		);

		mapRDD.foreach(
				x -> System.out.println("name:[" + x._1 + "] value:[" + x._2 + "]")
		);

		jsc.close();

	}

}
