package spark.SparkStreaming.kafkaDirectOffsetCommit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Serializable;

import java.util.*;


public class KafkaDirectOffsetCommit implements Serializable {

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

		/*
		当Kafka中没有初始offset或如果当前的offset不存在时（例如，该数据被删除了），该怎么办。
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
		//这里设置不自动提交依靠程序进行，消费的offset（偏移量）提交
		//因为如果自动提交的话，如果spark streaming没有即时消费到
		//数据就可能会丢失
		//通过程序获取当前这批kafka数据中的偏移量，程序处理完成后在进行提交，保证数据
		kafkaParams.put("enable.auto.commit", "false");

		//创建kafka的topic配置，存储集合可以存储多个，通过卡夫卡可以对多个topic进行并行化
		Set<String> topics = new HashSet<>(Arrays.asList("dsf"));

		//创建SpartStreaming的kafka输入流,"ConsumerRecord"
		// 这里就对上面的生产者对应的topic进行消费了
		/*
		LocationStrategies:

		新的Kafka(0.10) consumer API会将消息预取到缓冲区中。因此，出于性能原因，
		Spark集成保持缓存消费者对执行者（而不是为每个批次重新创建它们）是重要的，
		并且更喜欢在具有适当消费者的主机位置上调度分区。

		在大多数情况下，您应该使用LocationStrategies.PreferConsistent如上所示。
		这将在可用的执行器之间均匀分配分区。如果您的执行程序与Kafka代理所在的主机相同，
		请使用PreferBrokers，这将更喜欢在该分区的Kafka leader上安排分区。最后，
		如果您在分区之间的负载有显着偏差，请使用PreferFixed。这允许您指定分区到主机的
		显式映射（任何未指定的分区将使用一致的位置）。

		消费者的缓存的默认最大大小为64.如果您希望处理超过（64 *个执行程序数）Kafka分区，
		则可以通过以下方式更改此设置： spark.streaming.kafka.consumer.cache.maxCapacity

		缓存由topicpartition和group.id键入，因此对每个调用使用一个单独 group.id的createDirectStream。*/

		/*ConsumerStrateges:

		新的Kafka(0.10) consumer API有许多不同的方式来指定主题，其中一些需要相当多的
		后对象实例化设置。 ConsumerStrategies提供了一种抽象，允许Spark即使在从检查点
		重新启动后也能获得正确配置的消费者。

		ConsumerStrategies.Subscribe，如上所示，允许您订阅固定的主题集合。
		SubscribePattern允许您使用正则表达式来指定感兴趣的主题。注意，与0.8集成不同，
		在运行流期间使用Subscribe或SubscribePattern应该响应添加分区。最后，Assign
		允许您指定固定的分区集合。所有三个策略都有重载的构造函数，允许您指定特定分区的起始偏移量。*/

		JavaInputDStream<ConsumerRecord<String, String>> stream =
				KafkaUtils.createDirectStream(
						jssc,
						LocationStrategies.PreferConsistent(),
						ConsumerStrategies.Subscribe(topics, kafkaParams));


		/*Kafka有一个偏移提交API，将偏移存储在特殊的Kafka主题中。默认情况下，新消费者将定期自动提交偏移量。
		这几乎肯定不是你想要的，因为消费者成功轮询的消息可能还没有导致Spark输出操作，导致未定义的语义。这
		就是为什么上面的流示例将“enable.auto.commit”设置为false的原因。但是，您可以在使用commitAsyncAPI
		存储了输出后，向Kafka提交偏移量。与检查点相比，Kafka是一个耐用的存储，而不管您的应用程序代码的更改。
		然而，Kafka不是事务性的，所以你的输出必须仍然是幂等的。*/

		//使用KafkaUtils的createDirectStream方法，调用底层API直接消费Kafka Partition的数据
		// （Kafka Partition和RDD Partition 一一对应）。
		// createDirectStream返回值是DStream，底层是RDD。
			/*[plain] view plain copy
			val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

			[plain] view plain copy
					messages.foreachRDD 是对messages底层RDD计算其偏移范围。
					KafkaRDD和HasOffsetRanges关系(构造参数和泛型省略，具体见源码)：
			[plain] view plain copy*/

		//KafkaRDD和HasOffsetRanges关系(构造参数和泛型省略，具体见源码)：
		/*KafkaRDD extends RDD[R](sc, Nil) with Logging with HasOffsetRanges*/

		//rdd是messages.foreachRDD中的变量，rdd其类型是KafkaRDD，但是由于多态的原因rdd实际上不是KafkaRDD类型，
		//而是RDD类型，所以需要向下转型为HasOffsetRanges，调用offsetRanges方法。
		// （在OffsetRange是对什么内容的封装？答案：topic名字，分区Id，开始偏移，结束偏移。）
		/*	[plain] view plain copy
					val offsetsList: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
					offsetRanges 的实现代码（KafkaRDD中）：tp：TopicAndPartition,fo:fromOffset
							[plain] view plain copy
					val offsetRanges = fromOffsets.map { case (tp, fo) =>
						val uo = untilOffsets(tp)
						OffsetRange(tp.topic, tp.partition, fo, uo.offset)
		}.toArray*/


		stream.foreachRDD(rdd ->
		{
			if(!rdd.isEmpty()){

				//获得偏移量
				/*注意类型转换HasOffsetRanges只会成功，如果是在第一个方法中调用的结果createDirectStream，
				不是后来一系列的方法。请注意，RDD分区和Kafka分区之间的一对一映射在任何随机或重新分区的方法
				（例如reduceByKey（）或window（））后不会保留。*/
				OffsetRange[] offsetRanges = ((HasOffsetRanges)rdd.rdd()).offsetRanges();

				JavaRDD<String> dataRDD = rdd.map(record -> record.value());
				TakeInKafkaDirectWordCount.KafkaDirectWordCount(dataRDD);

				//更新已处理的偏移量提交给kafka
				((CanCommitOffsets)stream.inputDStream()).commitAsync(offsetRanges);
			}
		});

		//下面的写法逻辑和上面lambda的写法一样，只是没有用lambda的写法，但是不明白为什么会有
		//kafka序列化的问题，先放着以后了解深了在回来看看，不能停滞不前 2018.01.12
		/*java.io.NotSerializableException:
		Object of org.apache.spark.streaming.kafka010.DirectKafkaInputDStream
		is being serialized  possibly as a part of closure of an RDD operation.
		This is because  the DStream object is being referred to from within
		shapeless.the closure.  Please rewrite the RDD operation inside this DStream
		to avoid this.  This has been enforced to avoid bloating of Spark tasks  with
		unnecessary objects.

		stream.foreachRDD	(
				new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
					private static final long serialVersionUID = 4437451898741529438L;

					@Override
					public void call(JavaRDD<ConsumerRecord<String, String>> x) {
						if (!x.isEmpty()) {

							OffsetRange[] offsetRanges = ((HasOffsetRanges) x.rdd()).offsetRanges();

							JavaRDD<String> rdd = x.map(
									new Function<ConsumerRecord<String, String>, String>() {

										private static final long serialVersionUID = 6476368188730509305L;

										public String call(ConsumerRecord<String, String> x) {
											return x.value();
										}
									}
							);
							TakeInKafkaDirectWordCount.KafkaDirectWordCount(rdd);
							((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);

						}
					}
				}
		);*/

		jssc.start();
		jssc.awaitTermination();
		//jssc.stop();
	}
}

