package spark.SparkStreaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;


public class JavaKafkaDirectWordCount implements Serializable {

	public static void main(String[] args) throws Exception {
		KafkaDirect();
	}

	public static void KafkaDirect() throws Exception {
		SparkConf sparkConf = new SparkConf()
				.setAppName("KafkaDirectTest")
				.setMaster("local[2]");

		//创建SparkStreaming上下问类，拉取时间间隔5s
		JavaStreamingContext jssc  = new JavaStreamingContext(sparkConf, Seconds.apply(10));

		//创建连接kafka的消费者配置
		Map<String,Object> kafkaParams = new HashMap<>();

		//kafka的创建生产者节点连接地址，可以配置多个
		/*bootstrap.servers:

			host/port,用于和kafka集群建立初始化连接。
			因为这些服务器地址仅用于初始化连接，并通过现有配置的来发现全部的kafka集群成员（集群随时会变化），
			所以此列表不需要包含完整的集群地址（但尽量多配置几个，以防止配置的服务器宕机）。
		 */
		kafkaParams.put("bootstrap.servers","192.168.42.24:9092");

		//dsf-consumer-group
		/*
		此消费者所属消费者组的唯一标识。如果消费者用于订阅或offset管理策略的组管理功能，则此属性是必须的。
		指明当前消费进程所属的消费组，一个partition只能被同一个消费组的一个消费者消费
		 */
		kafkaParams.put("group.id","dsf-consumer-group");
		//kafka自带的反序列化类StringDeserializer.class
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);


		/*当Kafka中没有初始offset或如果当前的offset不存在时（例如，该数据被删除了），该怎么办。
		最早：自动将偏移重置为最早的偏移
		最新：自动将偏移重置为最新偏移
		none：如果消费者组找到之前的offset，则向消费者抛出异常
		其他：抛出异常给消费者*/
		kafkaParams.put("auto.offset.reset", "latest");


		//是否自动周期性提交，已经拉取到消费端的消息offset
		/*
		当Kafka中没有初始offset或如果当前的offset不存在时（例如，该数据被删除了），该怎么办。
		最早：自动将偏移重置为最早的偏移
		最新：自动将偏移重置为最新偏移
		none：如果消费者组找到之前的offset，则向消费者抛出异常
		其他：抛出异常给消费者。
		 */
		//如果为true，消费者的offset将在后台周期性的提交
		// 这里设置不自动提交依靠程序进行，消费的offset（偏移量）提交
		//因为如果自动提交的话，如果spark streaming没有即时消费到
		//数据就可能会丢失
		//通过程序获取当前这批kafka数据中的偏移量，程序处理完成后在进行提交，保证数据
		kafkaParams.put("enable.auto.commit", "false");

		//创建kafka的topic配置，存储集合可以存储多个
		Set<String> topics = new HashSet<>(Arrays.asList("dsf"));

		//创建SpartStreaming的kafka输入流,"ConsumerRecord"
		// 这里就对上面的生产者对应的topic进行消费了
		JavaInputDStream<ConsumerRecord<String, String>> stream =
				KafkaUtils.createDirectStream(
						jssc,
						LocationStrategies.PreferConsistent(),
						ConsumerStrategies.Subscribe(topics, kafkaParams));


		//源码中的例子：
		JavaDStream<String> lines = stream.map(ConsumerRecord::value);

		JavaPairDStream<String,Integer> wordCount = lines.flatMap(
				new FlatMapFunction<String, String>() {
					private static final long serialVersionUID = -7089861877101862325L;

					@Override
					public Iterator<String> call(String s) {
						return Arrays.asList(s.split(",")).iterator();
					}
				}
		).mapToPair(
				new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 5930545001436118134L;

					@Override
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<>(s,1);
					}
				}
		).reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = -2781584628282319160L;

			@Override
			public Integer call(Integer integer, Integer integer2)  {
				return integer+integer2;
			}
		}
		);

		wordCount.toJavaDStream().foreachRDD(
				new VoidFunction<JavaRDD<Tuple2<String, Integer>>>() {
					private static final long serialVersionUID = 7500557167517357379L;

					@Override
					public void call(JavaRDD<Tuple2<String, Integer>> t) {
						JavaPairRDD<String,Integer> j = t.mapToPair(
								new PairFunction<Tuple2<String, Integer>, String, Integer>() {
									private static final long serialVersionUID = 1753589917691946238L;

									@Override
									public Tuple2<String, Integer> call(Tuple2<String, Integer> t2) throws Exception {
										return new Tuple2<>(t2._1,t2._2);
									}
								}
						);

						j.foreach(new VoidFunction<Tuple2<String, Integer>>() {
							private static final long serialVersionUID = 8917221168324495417L;

							@Override
							public void call(Tuple2<String, Integer> j2) {
								System.out.println("key: " + j2._1 + "value: " + j2._2);
							}
						});
					}
				}
		);

		jssc.start();
		jssc.awaitTermination();
		//jssc.stop();
	}
}

