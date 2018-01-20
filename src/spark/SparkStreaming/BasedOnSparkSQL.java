package spark.SparkStreaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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
 * SparkStreaming真正强大的原因在于，它本身是在Spark的生态技术站中，
 * 可以无缝的和SparkCore和SparkSQL使用。
 *
 * 而且在Spark2.0中对于SparkSQL新推出了基于结构化的StructuredStreaming（结构化流）
 */

public class BasedOnSparkSQL {

	/**
	 * 基于SparkStreaming使用SparkSQL,
	 * 进行batch为2秒的每10秒统计前60秒窗口中的数据
	 * 并使用SparkSQL内置开窗函数取top3
	 *
	 * 需求：
	 * 针对所有bus的yyyymmddhh24,每小时频率统计，并取top3
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		SparkConf conf = new SparkConf().setAppName("StreamingOnSparkSQL").setMaster("local");

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

		//1.先获取元数据,同时根据 "busid+每日（201404）上午每小时" 做成key，
		//2.然后根据<busid的key,1>结合reduceByKeyAndWindow计算key的count
		//3.对于计算好的结果用javabean来，做成Dataset
		//4.在结合SparkSQL的sql内置开窗函数进行根据busid，取top3的数据

		//1-2
		JavaPairDStream<String,Integer> busDStreamRDD = stream.mapToPair(
				x -> (
						//做成<BUSID_时间,1>的元祖
						new Tuple2<>(
								//这里进行拼接的busid_时间（去整个时间串中YYYYMMDDHH24的格式）
								(x.value().split(",")[6]) + "_" +
									(x.value().split(","))[2].substring(0,10),1
						)
			)
		).reduceByKeyAndWindow(
				//每十秒处理前六十秒的数据
				(i1,i2) -> (i1+i2),Durations.seconds(60),Durations.seconds(10)
		);

		//3-4
		busDStreamRDD.foreachRDD(
				//将x中的<BUSID_时间,count>循环遍历
				x -> {
					//对x中的元祖进行map操作，取出其中连接的BUSID_时间数据，以及count值
					JavaRDD<Row> rowRDD = x.map(
							y ->(
									//上面因为是通过busid_time进行的连接，这里进行按"_"匹配下标拆分
									//成两个字段数据，再组合count，进行Row对象创建
									RowFactory.create(y._1.split("_")[0],
													  y._1.split("_")[1],
													  y._2
									)
								)
					);
					//做schema
					List<StructField> fields = Arrays.asList(
							DataTypes.createStructField("busID", DataTypes.StringType,true),
							DataTypes.createStructField("beginTime", DataTypes.StringType,true),
							DataTypes.createStructField("count", DataTypes.IntegerType,true)
					);
					StructType schema = DataTypes.createStructType(fields);

					//SQLContext sqlContext = new SQLContext(rowRDD.context());
					//2.X
					SparkSession spark = JavaSparkSessionSingleton.getInstance(rowRDD.context().getConf());
					//做成Dataframe
					Dataset<Row> busTimeCountDF = spark.createDataFrame(rowRDD,schema);

					//拼装执行SparkSQL语句并打印结果
					busTimeCountDF.createOrReplaceTempView("busCount");

					//使用内置函数"开窗函数"，根据busID分组排序connt子查询，取出排序倒序<=3
					Dataset<Row> top3DF = spark.sql(
							"select busID,beginTime,count from " +
									"(" +
									"select busID,beginTime,count," +
									"row_number() over " +
									"(partition by beginTime order by count desc) rank " +
									"from busCount" +
									") tmp " +
									"where rank <=3"
					);
					top3DF.show();
					/*
							+--------+----------+-----+
							|   busID| beginTime|count|
							+--------+----------+-----+
							|00013532|2014040116|  191|
							|00013156|2014040116|   68|
							|00017542|2014040116|   48|
							|00013532|2014040117|  233|
							|00017512|2014040117|   84|
							|00069624|2014040117|   76|
							|00017505|2014040108|   56|
							|00016386|2014040108|   32|
							|00017542|2014040108|   12|
							|00015858|2014033119|   20|
							|00017481|2014040119|   56|
							|00069793|2014040119|   28|
							|00017521|2014040119|   20|
							|00065545|2014040111|   32|
							|00017499|2014040111|   16|
							|00017505|2014040111|    8|
							|00017542|2014040112|   48|
							|00065545|2014040112|   48|
							|00051026|2014040112|   36|
							|00069636|2014040118|  220|
							+--------+----------+-----+
									only showing top 20 rows
							*/

				}

		);
		jssc.start();
		jssc.awaitTermination();
		jssc.close();

	}
}

/** Lazily instantiated singleton instance of SparkSession */
/*
	还是官方会玩，这里创建一个懒加载，提升资源利用率，而且还能共用一个SparkConf
	官方的例子还是很经典使用的，还是都要多看看才行。20180114
 */
class JavaSparkSessionSingleton {
	private static transient SparkSession instance = null;
	public static SparkSession getInstance(SparkConf sparkConf) {
		if (instance == null) {
			instance = SparkSession
					.builder()
					.config(sparkConf)
					.getOrCreate();
		}
		return instance;
	}
}
