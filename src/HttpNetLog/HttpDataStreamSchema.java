package HttpNetLog;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class HttpDataStreamSchema {

	private static final Logger logger = LoggerFactory.getLogger(HttpDataStreamSchema.class);

	public static void StreamSpark() throws InterruptedException {

		//创建SparkConf
		SparkConf conf = new SparkConf()
				.setAppName("dsfStreaming")
				//.setMaster("local")
				.set("spark.streaming.concurrentJobs", "5") //五个并行化
				;

		//创建SparkStreaming,连接间隔10s
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Seconds.apply(10
		));

		Map<String,Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers","192.168.42.24:9092");
		kafkaParams.put("group.id","dsf-consumer-group");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", "false");//先让kafka自动提交，方便测试
		Set<String> topics = new HashSet<>(Arrays.asList("dsf"));

		logger.info("kafka配置初始化完成");

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
				jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.Subscribe(topics, kafkaParams)
		);

		logger.info("JavaInputDStream数据接入初始化完成");

		HttpDatatoHiveSchema datatoHive = new HttpDatatoHiveSchema();

		stream.foreachRDD(rdd ->
		{
			if(!rdd.isEmpty()){
				//获得偏移量
				OffsetRange[] offsetRanges = ((HasOffsetRanges)rdd.rdd()).offsetRanges();
				//优化数据结构，把List修改成数组结构减少内存使用
				//JavaRDD<List<String>> dataRDD = rdd.map(record -> Arrays.asList(record.value().split("\t")));
				JavaRDD<String[]> dataRDD = rdd.map(record ->record.value().split("\t"));
				datatoHive.save(dataRDD);
				//更新已处理的偏移量提交给kafka
				((CanCommitOffsets)stream.inputDStream()).commitAsync(offsetRanges);
			}
		logger.info("本次数据RDD大小:"+ rdd.count());
		});

		jssc.start();
		logger.info("SparkSteraming 启动");
		jssc.awaitTermination();
	}
}
