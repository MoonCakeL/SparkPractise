package HttpNetLog;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.util.*;

public class HttpDataStream {
	public static void main(String[] args) throws InterruptedException {

		//创建SparkConf
		SparkConf conf = new SparkConf()
				.setAppName("dsfStreaming")
				.setMaster("local")
				.set("spark.streaming.concurrentJobs", "5")
				;

		//创建SparkStreaming,连接间隔5s
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Seconds.apply(10));
		//Durations.seconds(5);

		Map<String,Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers","192.168.42.24:9092");
		kafkaParams.put("group.id","dsf-consumer-group");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", "false");//先让kafka自动提交，方便测试
		Set<String> topics = new HashSet<>(Arrays.asList("dsf"));

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
				jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.Subscribe(topics, kafkaParams)
		);

		HttpDatatoHive datatoHive = new HttpDatatoHive();

		stream.foreachRDD(rdd ->
		{
			if(!rdd.isEmpty()){
				//获得偏移量
				OffsetRange[] offsetRanges = ((HasOffsetRanges)rdd.rdd()).offsetRanges();
				JavaRDD<List<String>> dataRDD = rdd.map(record -> Arrays.asList(record.value().split("\t")));
				datatoHive.save(dataRDD);
				//更新已处理的偏移量提交给kafka
				((CanCommitOffsets)stream.inputDStream()).commitAsync(offsetRanges);
			}
		});


		jssc.start();
		jssc.awaitTermination();
	}
}
