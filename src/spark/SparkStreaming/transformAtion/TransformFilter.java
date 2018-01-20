package spark.SparkStreaming.transformAtion;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

/**
 * transform操作
 * 	transform算子本身原始DStream中特有的算子，DStream的强大之处在于，可以用于执行
 * 	任意的RDD到RDD的转换操作(因为DStream有很多基本RDD没有的算子操作，
 * 	所以使用transform，在内部进行RDD到RDD的函数，作用在每一个DStream上，得到一个
 * 	新的 DStreamRDD)。
 *
 * 	它可以用于实现，DStreamAPI中所没有的操作。
 * 	如：
 * 		DStrem本身是来自一个个batch中的DStreamRDD，DStream的join()方法，
 * 		让DStreamRDD只能和StreamRDD之间进行join算子操作，
 * 		无法于一个"特定"的RDD进行join操作。
 *
 * 		但使用transform通过将RDD到RDD的映射函数func作用于原DStream中的每一个RDD得到一个新DStream。
 * 		所以在每一个batch中DStreamRDD可以通过transform算子，可以和一个RDD进行join。
 */
public class TransformFilter {
	/**
	 * 使用DStreamAPI中transform()算子，和一个特定的RDD进行join操作，
	 * "过滤"每一个batch中DStreamRDD，在一个特定RDD中的不包含（或包含）的记录
	 *
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		SparkConf conf = new SparkConf().setAppName("UpdateStateByKeyWordCount").setMaster("local");

		JavaStreamingContext jssc = new JavaStreamingContext
				(conf, Durations.seconds(10));

		//创建连接kafka的消费者配置
		Map<String,Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers","192.168.42.24:9092");
		kafkaParams.put("group.id","dsf-consumer-group");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", "true");//先让kafka自动提交，方便测试
		Set<String> topics = new HashSet<>(Arrays.asList("dsf"));

		//people.txt
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
				jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.Subscribe(topics, kafkaParams)
		);

		//定义一个过滤的列表内容
		List<Tuple2<String,Boolean>> filterList = new ArrayList<>();

		filterList.add(new Tuple2<>("Michael",true));
		filterList.add(new Tuple2<>("Justin",true));

		//转换成为一个JavaRdD，之后用于"过滤列表RDD"和原始数据中的RD，先进行join操作，将"true"的判断
		//关联在一起，然后在通过filter算子，过滤列表中为"true"的key，进行过滤

		//这里需要说明的是jssc在内部会自己创建一个sparkContext()方法这里其实就是JavaSparkContext
		//毕竟底层还是用java去和spark交互，所以作为spark给java提供的"入口"JavaSparkContext，还是会
		//隐示创建的。
		JavaPairRDD<String,Boolean> filetrRDD = jssc.sparkContext().parallelizePairs(filterList);
		//System.out.println(filetrRDD.collect());

		//获取数据
		//对于需要根据内容进行过滤，所以这里将原始数据中,需要进行过滤的内容做成key，用于之后通过这个key进行过滤
		JavaPairDStream<String,String> dataRDD = stream.map(
				//只获取ConsumerRecord<String, String>中的value部分
				x -> x.value()
				//通过对整体内容利用逗号分组，返回的是一个数组，
				//然后对数组中，需要过滤的内容，取数组中第二个下标位置的内容作为key
		).mapToPair(x -> new Tuple2<>(x.split(",")[1],x)
		);
		/*
		dataRDD.print();
		(Michael,1,Michael,29,男)
		(Andy,2,Andy,30,女)
		(Justin,3,Justin,19,男)
		*/

		//通过上面的transformation过程，接下来就可以通过DStream的transform算子进行join、filter、map算子
		//进行于过滤列表的关联，就可以获取过滤列表中的key和元数据中的key，的boolean的值的对应关系的rdd
		//然后在通过filter算子过滤boolean中的true，过滤掉元数据中的key
		//最后通过map算子取得rdd中只包含数据，不要boolean的部分完成最终的过滤过程
		//接下来的transfor算子部分用java的匿名类编写，可以清楚的看清逻辑

		//定义最终的javardd类型
		JavaDStream<String> filtedDataRDD = dataRDD.transform(
				new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
					private static final long serialVersionUID = -35810611076664359L;

					@Override
					public JavaRDD<String> call(JavaPairRDD<String, String> x)  {

						//注意这里使用左外连接进行join，因为过滤列表的目的是过滤掉，符合自己列表中key以及为true的部分
						//原始rdd中的数据不在列表中的还是要保留，如果是join那就相当于"=="
						JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinRDD
								= x.leftOuterJoin(filetrRDD);
						/*
						Michael,1,Michael,29,男,true
						Andy，2,Andy,30,女,null
						Justin，3,Justin,19,男,true*/

						//完成左外连接后，说明原始rdd已经和过滤列表中byKey的部分已经关联上了，
						// 通过filter算子过滤掉为"true"的部分

						//这里需要说明的是因为处理环节多，一定要分清楚
						//在Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>中
						//每一个元素中的数据是什么，不然最后在每一个元祖中取什么都很懵

						/*JavaPairRDD<String（上面用于逗号分割通过下标为1的数据做的key）
						 , Tuple2<String, Optional<Boolean>> 这个元祖里面的string才是实际我们需要的数据，
						 而第二个Optional<Boolean>是存放着对应这个rdd中的第一个string（key）的判断"true">*/
						JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filtedJoinRDD=joinRDD.filter(
								//进入到这里Tuple2<String, Optional<Boolean>中的String是我们需要的数据
									new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
										private static final long serialVersionUID = 1490077998214978701L;

										@Override
										public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> b) {
											/*
											Optional<Boolean>：这里才是存放"true"的内容
											Optional这个类型这里是scala中的类型，但在jdk1.8中也有意义一样
											用于一个个能存在，又不可能存在的值的变量
											都是结合这使用，
											在这里就是结合着过滤内容，通过和元数据进行过滤内容关联，
											关联上的就是存在，没有关联的也就是说不需要过滤的，就会不存在
											*/
											//结合filter算子，取b元素中，tuple2中的第二个元素，tuple2中的第二个
											//Optional<Boolean>>才是判断条件，首先它不为null
											//同时，它的内容为（做过滤数据是设置为true了）"true"，
											//就说明是我们需要过滤掉不要的数据，那么就返回false，过滤掉，其他的返回true就好
											if (b._2()._2().isPresent() && b._2()._2().get()){
												return false;
											}
											return true;
										}
									}
							);
						//通过上面过滤，已经将符合过滤列表中的"key"的数据过滤掉了
						//但是到这里的rdd中的存储的内容结构为：
						//JavaPairRDD<String, Tuple2<String, Optional<Boolean>>>

						/*Andy，2,Andy,30,女，null*/

						//第一个string是为了过滤做的key，第二个tuple2中的第一个元素，才是
						//整个字符串数据的全部内容
						//这里通过循环，只取这个RDD中第二个元素的Tuple2元祖中的第二个元素就可以
						JavaRDD<String> dStream = filtedJoinRDD.map(
							f -> f._2._1
						);

						/*System.out.println(dStream.collect());
						[2,Andy,30,女]*/
						return dStream;
					}
				}
		);

		//打印结果，完成过滤内容的过滤
		filtedDataRDD.print();

		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}
