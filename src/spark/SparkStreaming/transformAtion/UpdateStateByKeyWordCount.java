package spark.SparkStreaming.transformAtion;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

/**
 * updateStateByKey算子，是对于SparkStreaming中的特殊算子
 * 它可以让我们为每一个key维护一个state（状态），并且持续不断的
 * 更新该state（累计）。
 	* 1.首先，需要定义一个state变量，可以是任意的数据类型
 	* 2.其次，需要定一个state更新算法（累计的算法逻辑）
 * 		指定一个算法如何使用之前的state和新值来更新的state。
 *
 * 	说明：对于每个batch，"Spark都会为每个之前已存在的key，
 * 		应用一次state的更新算法，新出现的key也会进行一次state的
 * 		更新算法"，无论这个key在batch中是否有新数据。
 * 		如果对state更新算法中，state更新函数返回none，那么
 * 		这个key对应的state就会被删除。
 *
 * 	注意：使用updateStateByKey算子，spark要求必须开启CheckPoint
 * 		机制，用于存储每次state更新的值，以及当SparkStreaming故障
 * 		宕机时，内存中的数据丢失，可以从checkpoin（HDFS）中进行恢复。
 */
public class UpdateStateByKeyWordCount {

	/**
	 * 使用SparkStreaming的updateStateByKey，
	 * 进行分词计数累计。
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setAppName("UpdateStateByKeyWordCount").setMaster("local");

		JavaStreamingContext jssc = new JavaStreamingContext
				(conf,Durations.seconds(10));

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
				jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.Subscribe(topics, kafkaParams)
		);

		//启动checkpoint，将数据保存到hdfs上
		//但需要注意的是一旦SparkStreaming本身或者SparkStreaming的应用程序"重新编译了"
		//那么SparkStreaming就必须重新创建StreamingCcontext或者JavaStreamingContext，
		//就无法重新基于checkpoint点进行重建。
		//这是因为checkpoint记录点的数据是"类"序列化后的结果，因此重建的时候需要反序列化，
		//但类重新编译了，于之前checkpoint记录点的编译前类的序列化不一致，那么就会在重建时
		//序列化失败。
		//所以在类重新编译后，必须确保检查点的文件再每次重新编译代码后，需要启动新代码之前要显式删除。
		jssc.checkpoint("hdfs://192.168.42.24:9000/data/checkpoint");

		//获取ConsumerRecord中value的部分，然后进行按逗号分割(开始尝试简单的lambda的写法2018.01.12)
		JavaPairDStream<String,Integer> word = stream.map(x -> x.value())
				.flatMap(x -> Arrays.asList(x.split(",")).iterator())
				.mapToPair( x -> new Tuple2<>(x,1)
						//(PairFunction<String, String, Integer>) s -> new Tuple2<>(s,1)
				);

		//接下来要使用DStream的updateStateByKey算子,
		//使用之前需要保证过来的是一个PairRDD才行，不然没有key，怎么基于key进行ByKey操作
			JavaPairDStream<String,Integer> updateStatewORKcount=word.updateStateByKey(
						/*new Function2<List<Integer>, Optional<? extends Object>, Optional<? extends Object>>() {
							@Override
							public Optional<? extends Object> call(List<Integer> integers, Optional<?> optional) throws Exception {
								return null;
							}
						}*/
						//这里的Optional，相当于Scala中的样例类，相当于Option。
						//可以这样理解，它代表了一个"值"的"存在状态"，可能存在也可能不存在。
						new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
							private static final long serialVersionUID = -3237785544877118704L;

							@Override
							//这里就是updateStateByKey算子中"state更新算法"，每次batch都会通过这里进行计算
							public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {
								/*内部方法的两个变量说明：

				（本次batch中出现的次数）values：就是本次这次batch中，基于这个key，在这次batch中出现次数累加的值，
											类型为List的原因是因为，一个词可能会出现多次，
											例如（hello,1）,（hello,1）,所以是一个list。

	（全局状态（并包含全局次数累加的值））state：本身存储这对于这个key，在全局计算中（程序运行过程中），的全局状态，
											每次每个batch中都进到内部算法中都是"当前最新"的全局状态。

											Optional<Integer>>类型中同时存储着这个次，在全局中累计出现的个数
											其中的泛型需要自己指定。
								*/

								 //1.先定义一个用来全局进行存储，单词计数的变量
								 Integer newValue = 0;
								 //2.进行判断：
								// 对进入到state算法中的这个key的state，是否以及出现进行判断
								// 如果以及是存在的，说明以及出现过了，之前对这个key，已经统计过全局的次数，
								// 那么就把本次的这个"最新的"，"全局的"state状态中值赋给这个key的计数次数。
								if(state.isPresent()){
									newValue = state.get();
								}

								//如果不存在，说明之前没有出现过，对于这个key也就没有，
								// 这个key对应的"全局的state
								// 那么就把本次batch中这个值出现的所有次数相加，
								// 这样也就有了这个key对应的它自己的"全局state"

								// list中的次数循环相加给newValue
								for (Integer value : values){
									newValue +=value;
								}
								//并返回这个新出现的key对应的"全局的state"中key出现的次数（value）
								return Optional.of(newValue);
							}
						}
				);

		//updateStatewORKcount.print();
		//第一层foreachRDD，是循环的是每一个RDD
		//第二层的foreachRDD才可以对进来的RDD，取其中的元祖
		updateStatewORKcount.foreachRDD(
				x -> x.foreach(x1 ->
						System.out.println("key:[" + x1._1+"~updateValue:[" +  x1._2+"]")
		));

	jssc.start();
	jssc.awaitTermination();
	jssc.close();
	}
}
