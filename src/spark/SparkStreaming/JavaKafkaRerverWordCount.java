package spark.SparkStreaming;

/**
 * Spark在1.3之后放弃了Rerver这种方式，而使用了createDirectStream
 *  这中方式解决了
 *  1.多级目录读取文件
 *  2.spark的partition和kafka的partition可以自动完成一对一的并行化
 *  3.Rerver的方式需要开启WAL(kafka自带高可靠机制)，Direct不需要减少了io和网络消耗
 *  4.一次且仅一次的事物机制
 *  	基于Rerver这种方式，是使用kafka的高阶api来在zookeeper中保存了消费过的offset
 *  	这是kafka消耗数据的传统方式。这种配合着WAL机制可以保证数据零丢失，但无法保证数据
 *  	被处理一次且仅仅一次。可能会处理两次。因为spark和zookeeper之间可能是不同步的。
 *
 *  	而Direct的方式，使用kafka的简单api，Spark Streaming自己就负责消费跟踪offset
 *  	并保存在checkpoint中.Spark自己同步一次，因此可以保证数据是同步消费且仅消费一次。
 */

/*import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class JavaKafkaRerverWordCount {

	*//**
	 * 基于kafka作为Rerver的实时wordcount
	 * @param args
	 *//*
	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf()
				.setAppName("JavaKafkaRerverStreaming").setMaster("local[2]");

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

		//kafkaTopic kafka的Topic线程数，用多少个线程去拉取kafka partition中的数据
		Map<String,Integer> kafkaTopicMap = new HashMap<String,Integer>();
		kafkaTopicMap.put("KafkaRerverTest",1); //kafka在zk中的topic配置（目录）/brokers/KafkaRerverTest

		//使用Kafk.Utils.createStream方法，创建针对kafka的输入数据流InputDStream
		//，直接就是一个JavaPairDStream<String,String>，不过元祖中的第一个值为null
		//KafkaUtils."createStream"而不是"createDirectStream"，
		JavaPairReceiverInputDStream<String,String> word = KafkaUtils.createStream(jssc,
									"192.168.42.24:2181",  //zkServer的地址
									"DefaultConsumerGroup", //kafka出入流的名字
									kafkaTopicMap			//kafka在zk中的topic配置（目录）
									);


		JavaDStream<String> wordDStream = word.flatMap(
				new FlatMapFunction<Tuple2<String, String>, String>() {
					private static final long serialVersionUID = -6054742334151437055L;

					@Override
					public Iterator<String> call(Tuple2<String, String> lines) {
						//元祖中的第一个值为null
						return Arrays.asList(lines._2.split(" ")).iterator() ;
					}
				}
		);

		JavaPairDStream<String,Integer> wordCount = wordDStream.mapToPair(
				new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1364300502891106038L;

					@Override
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<>(s,1);
					}
				}
		).reduceByKey(
				new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = -1019426160739292117L;

					@Override
					public Integer call(Integer integer, Integer integer2) {
						return integer + integer2;
					}
				}
		);

		wordCount.print();

		jssc.start();
		jssc.awaitTermination();;
		jssc.close();
	}

}*/
