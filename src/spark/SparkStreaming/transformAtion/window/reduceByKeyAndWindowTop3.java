package spark.SparkStreaming.transformAtion.window;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

public class reduceByKeyAndWindowTop3 {
	/**
	 * 每隔10秒计算一下，前60秒内聚合的wordRDD，并在窗口内进行显示top3；
	 *
	 * 这里需要说明的是batch为2s，窗口长度为60s，滑动间隔是10s
	 *
	 * 也就是说，每隔10s，将最近60秒的数据，作为一个窗口，通过内部聚合，
	 * 然后统一对一个聚合了60秒数据的windowRDD进行后续计。
	 *
	 * 所以说这里就是，在"reduceByKeyAndWindow"算子之前，都是不会立即
	 * 进行计算的，只是放在那里，然后在滑动间隔到了之后（10s），才会将之前
	 * 收集了60s的rdd进行统一的，在一个window窗口的计算，
	 * 所以说滑动间隔到了之后才会触发计算，之前都是在收集rdd
	 *
	 * 这里是2batch 60window 收集了15个rdd，形成一个windowRDD只用来聚合rdd
	 * ，然后放在那里，需要在10s的滑动间隔到了才执行，"reduceByKeyAndWindow"的计算。
	 *
	 * 同时"reduceByKeyAndWindow"算子只是针对这一次windown中的15个rdd进行计算
	 * 不是对一个完整的DStream进行计算
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf()
				.setMaster("local")
				.setAppName("reduceByKeyAndWindowTop3");

		//batch为2s，窗口间隔为6s，滑动间隔为12s
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));

		//创建连接kafka的消费者配置
		Map<String,Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers","192.168.42.24:9092");
		kafkaParams.put("group.id","dsf-consumer-group");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", "true");//先让kafka自动提交，方便测试

		Set<String> topics = new HashSet<>(Arrays.asList("dsf"));
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
				jssc, LocationStrategies.PreferConsistent(),
				ConsumerStrategies.Subscribe(topics, kafkaParams)
		);

		JavaPairDStream<String,Integer> wordCout =
			stream.map(
					x -> x.value()
			).flatMap(
					x -> Arrays.asList(x.split(",")).iterator()
			).mapToPair(
					x -> new Tuple2<>(x,1)
			).reduceByKeyAndWindow(
					//第二个时间参数，是窗口长度，这里是60s，进行60s内batch为2的，15个rdd聚合成一个windowRDD
					//第三个时间参数，是滑动长度，这里是10s，每隔10s对之前聚合60s的windowRDD进行reduceByKeyAndWindow

					//这里的batch时间是2s，2s拉取一次数据，相当于streamRDD
					// 然后60s的窗口时间中有15个2s，也就是有15个RDD进行了聚合之后形成一个windowRD，
					//滑动间隔为10s的时间间隔，进行算子计算
					//这里也就是说10s处理包含一个15个RDD的windowRDD的reduceByKeyAndWindow算子操作
					(i1,i2) -> (i1+i2), Durations.seconds(12),Durations.seconds(6)
					//(Function2<Integer, Integer, Integer>) (integer, integer2) -> integer+integer2,Durations.seconds(6),Durations.seconds(12)
			);
		//通过DStream的transfo算子在内部进行RDD转换，先将DStreamRDD反转为<value，string>的形式，（统计数字在前面）
		//然后使用基本RDD的sortByKey进行排序
		//然后再次反转回来变成原始的<string,value>的结果即可//在使用take(3)的算子取出top3排名
		JavaPairDStream<String,Integer> sortWordCountRDD = wordCout
				.transformToPair(
						//(Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>) stringIntegerJavaPairRDD -> {
						//还是lambda写法好看,但需要知道里面是什么东西，什么结果才能写出来，多看看idea转换的lambda多写写就好了20180113
						//这里需要使用{}来扩住内部的方法因为
						//可选的大括号：如果主体包含了一个语句，就不需要使用大括号。
						x -> {
							JavaPairRDD<Integer,String> sortByKeyRDD = x.mapToPair(
										xx -> new Tuple2<>(xx._2,xx._1)
								).sortByKey(false);

							//排序完成后再反转会原始的样子
							JavaPairRDD<String, Integer> dStream = sortByKeyRDD.mapToPair(
								y -> new Tuple2<>(y._2,y._1)
						);
							//到这里突然想起来take返回的是个List，
							List<Tuple2<String,Integer>> listTop3 = dStream.take(3);
							listTop3.stream()
									.forEach(
											yy -> System.out.println(
													"key:[" + yy._1 + "] value:[" + yy._2 +"]"
											)
									);

							//返回只能返回排序的pairRDD了，不然下面不出发DStream的action前面的transform都没有用							return dStream;
						return dStream;
						}

		/*new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
			@Override
			public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) {

				JavaPairRDD<Integer,String> sortByKeyRDD = stringIntegerJavaPairRDD.mapToPair(
						x -> new Tuple2<>(x._2,x._1)
				).sortByKey(false);

				//排序完成后再反转会原始的样子
				JavaPairRDD<String, Integer> dStream = sortByKeyRDD.mapToPair(
						x -> new Tuple2<>(x._2,x._1)
				);
				//到这里突然想起来take返回的是个List，
				List<Tuple2<String,Integer>> listTop3 = dStream.take(3);
				listTop3.stream()
						.forEach(
								x -> System.out.println(
										"key:[" + x._1 + "] value:[" + x._2 +"]"
								)
						);

				//返回只能返回排序的pairRDD了，不然下面不出发DStream的action前面的transform都没有用							return dStream;
				return dStream;
			}
		}*/
				);

		sortWordCountRDD.print();
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}
