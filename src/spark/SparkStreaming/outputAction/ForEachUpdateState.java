package spark.SparkStreaming.outputAction;

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

import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

public class ForEachUpdateState {
	/*
	create table test12 (word varchar(64),countValues int);
	alter table test12 add column saveTime TIMESTAMP;
	 */
	public static void main(String[] args) throws Exception{
		updateStateToJDBC();
	}

	public static void updateStateToJDBC() throws Exception {
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

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
				jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.Subscribe(topics, kafkaParams)
		);

		//启动checkpoint，将数据保存到hdfs上
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
				new Function2<List<Integer>, org.apache.spark.api.java.Optional<Integer>, org.apache.spark.api.java.Optional<Integer>>() {
					private static final long serialVersionUID = -3237785544877118704L;

					@Override
					//这里就是updateStateByKey算子中"state更新算法"，每次batch都会通过这里进行计算
					public org.apache.spark.api.java.Optional<Integer> call(List<Integer> values, org.apache.spark.api.java.Optional<Integer> state) {

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
		/*updateStatewORKcount.foreachRDD(
				x -> x.foreach(x1 ->
						System.out.println("key:[" + x1._1+"~updateValue:[" +  x1._2+"]")
				));*/
		updateStatewORKcount.foreachRDD(
				x -> {
					//进入到foreachRDD后，因为内部存储的是javaRDD，javaRDD才会有foreachPartition算子
					/*
					 这里使用了基于javaRDD的*partition的算子,原因是因为foreach和map这样的算子
					 内部的实现都是一个一个进行的，如果在内部使用过程中需要频繁创建新对象（new）
					 那么首先不会那么高效而且对资源造成一定程度的浪费。
					 而基于*partition的算子是将内部rdd中的每一个元素变成了rdd中每一个分区的迭代器
					 可以基于一个内部元素分区创建创建一个新对象，让分区内所有元素都可以共享通过一个对象
					 以便与提升性能，典型的就如同对mysql入库这样的，需要插入一条就创建一个jdbc，开销会比较大。

					 常用的算子中主要使用比较多的就是：mapPartitions算子和foreachPartition算子
					 ，当然还有几个也是基于partition的但是不常用，详细可以看javardd的源码class中的算子函数（在收藏中）
					 */
					x.foreachPartition(
							//lambda内部还有方法的话需要使用大括号
							y -> {
								//对每一个foreach的partition进行一次连接的创建，节省资源
								Connection conn = ConnectJDBCPool.getConnection();
								//foreachPartition中存储的元祖是个迭代器，所以要循环取出来，这里创建一个
								//临时变量
								Tuple2<String,Integer> tuple2 = null;

								//每次循环后进行插入
								while (y.hasNext()){
									tuple2 = y.next();

									/*s -> "insert into test12 (word,countValues) " +
											"values('" + tuple2._1 + "," + tuple2._2 + ")";*/
								String sql = "insert into test12 (word,countValues) " +
										"values ('" + tuple2._1 + "'," + tuple2._2 + ")";

									//创建执行器接口，执行sql（单独写了一个Mysqljdbc的连接池,用于控制并发）
									/*
									这里需要着重说明的就是，对于新对象的创建应当放在循环的最内层
									而不是最外层！！！
									 */
									Statement stmt = conn.createStatement();
									stmt.executeUpdate(sql);

								}
								//完成整个partition的数据插入后，归还连接
								ConnectJDBCPool.returnConnection(conn);
							}
					);
				}
		);


		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}
