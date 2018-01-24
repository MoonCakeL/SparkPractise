package sparkElasticsearch.examples.RDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ReadingDataFromEsByJava {
	private static final Logger logger = LoggerFactory.getLogger(ReadingDataFromEsByJava.class);
	private static final  SparkConf sparkConf = new SparkConf()
			.setMaster("local")
			.setAppName("Reading Data From ES")
			//Spark去进行Elasticsearch的连接方式,与HBase不一样，
			//HBase是使用	JavaHBaseContext进行连接，然后执行对HBase的方法
			//而Elasticesearch是直接通过对SparkConf进行设置，
			//Elasticsearch没有类似JavaEsContext的连接方法
			//而对是使用JavaEsSpark方法，然后和连接HBase一样把jsc的连接扔进去

			//不同的是JavaHBaseContext方法中是分别添加jsc和hbase自己的连接工厂方法
			//而Elasticsearch是将自己的连接放到SparkConf中，jsc在加载SparkConf，
			// 然后通过一个参数在JavaEsSpark方法中添加包含了spark和es的连接
			.set("es.nodes","192.168.42.24,192.168.42.25,192.168.42.26")
			.set("es.port","9200");

	//添加了包含连接Elasticsearch连接信息的SparkConf
	//jsc就可以进行连接操作Elasticsearch
	private static final JavaSparkContext jsc = new JavaSparkContext(sparkConf);



		public static void main(String[] args){
			//esPairRDD();
			esRDD();
		}


		public static void esPairRDD(){

		JavaPairRDD<String,Map<String,Object>> esPairRDD =
				JavaEsSpark.esRDD(
						jsc,
						//_index/_type
						"estest/20180123"
					);

		/* 在Elasticsearch中的json数据
		"hits": {
			"total": 2,
					"max_score": 1,
					"hits": [
			{
				"_index": "estest",
					"_type": "20180123",
					"_id": "AWEix04TP0YuplwDnVY2",
					"_score": 1,
					"_source": {
				"id": "123456",
						"name": "x1",
						"age": "30",
						"checkintime": "201801231925"
			}
			},
			{
				"_index": "estest",
					"_type": "20180123",
					"_id": "AWEiyKoJP0YuplwDnVY3",
					"_score": 1,
					"_source": {
				"id": "123456",
						"name": "x1",
						"age": "30",
						"checkintime": "201801230000"
			}
			}
    ]
		}*/

		try {
			esPairRDD.foreach(
				x -> {
					System.out.println("id:" + x._1);
					System.out.println("====");

					for (Map.Entry<String,Object> entry : x._2.entrySet()){
						System.out.println("MapKey:"+ entry.getKey());
						System.out.println("MapValue JSON:");
						System.out.println("Document:"+ entry.getValue());
					}

				}
		);
		}finally {
			jsc.close();
		}
		/* 查询返回的结果
		id:AWEix04TP0YuplwDnVY2
				====
		MapKey:id
		MapValue JSON:
		Document:123456
		MapKey:name
		MapValue JSON:
		Document:x1
		MapKey:age
		MapValue JSON:
		Document:30
		MapKey:checkintime
		MapValue JSON:
		Document:201801231925
		id:AWEiyKoJP0YuplwDnVY3
				====
		MapKey:id
		MapValue JSON:
		Document:123456
		MapKey:name
		MapValue JSON:
		Document:x1
		MapKey:age
		MapValue JSON:
		Document:30
		MapKey:checkintime
		MapValue JSON:
		Document:201801230000*/
		//从以上结果中来看默认情况下,如果只对一个_index下的一个_type查询
		//ES将返回一个JavaPairRDD<String,Map<String,Object>>的Tuple2的rdd结构
		//其中：
		// 	PairRDD中的key为:indexid
		//	Tuple2元祖中的 t_1 为实际_document中的field字段
		//	Tuple2元祖中的 t_2 为实际_document中的field字段对应的value

	}

	public static void esRDD(){
		JavaRDD<Map<String,Object>> esRDD = JavaEsSpark
				.esRDD(
						jsc,
						//_index/_type
						"estest/20180123",
						"?q=x*"
				).values();

		esRDD.foreach(
					x ->{
						for (Map.Entry entry : x.entrySet()){
							System.out.println(
									"Document.Field:"+ entry.getKey() +
									"Document.Value:"+ entry.getValue()
							);
						}
					}
		);

		//从这里不难看出如果在JavaEsSpark()方法后面添加.values()方法
		//就限定只需要_index中Document的部分，但依然是个Map<String,Object>
		//Map中的内容不变，只是value方法过滤掉了indexid

		//同时JavaEsSpark.esRDD()方法中，可以传递第三个参数为query过滤条件参数
		//对于写法依然是使用ES的DSL语言进行的查询

		//"?q=x*" 的意义为：
		// "?q" 部分表示是query查询
		// "=" 为表达式
		// x* 表示查询字符串是x开头的前模糊查询
/*
		Document.Field:idDocument.Value:123456
		Document.Field:nameDocument.Value:x1
		Document.Field:ageDocument.Value:30
		Document.Field:checkintimeDocument.Value:201801231925
		Document.Field:idDocument.Value:123456
		Document.Field:nameDocument.Value:x1
		Document.Field:ageDocument.Value:30
		Document.Field:checkintimeDocument.Value:201801230000
				*/

		//其实在执行查询的时候看后台打印的es查询info就可以很清楚的看到
		// ES存储数据的集群架构的原因和查询的步骤
		//大概分为
		//1 先提交一个json的 appliction的查询请求，使用Head方法检查查询_index是否存在
		/*Added HTTP Headers to method: [Content-Type: application/json
				, Accept: application/json
]
		2018-01-24 21:02:16,962 DEBUG [main] org.apache.commons.httpclient.Wire.wire(70) | >>
		"HEAD /estest HTTP/1.1[\r][\n]"
		"Content-Type: application/json[\r][\n]"
		 "Accept: application/json[\r][\n]"
		 "User-Agent: Jakarta Commons-HttpClient/3.1[\r][\n]"
		 "Host: 192.168.42.24:9200[\r][\n]"
		 "[\r][\n]"
		 "HTTP/1.1 200 OK[\r][\n]"
		 "HTTP/1.1 200 OK[\r][\n]"
		"content-type: application/json; charset=UTF-8[\r][\n]"
		"content-length: 590[\r][\n]"
		 "[\r][\n]"
		 查询结果返回："HTTP/1.1 200 OK[\r][\n]"，说明_index是存在的
		 */

		//2 使用"GET /_cluster/health/estest 检查集群内所有node的状态
		//	因为es存储是按分片进行的存储，每个分片在不同的node上，所以要先检查各个节点状态,
		//	同时检查需要查询的_index在那个节点中
		/*
			"GET /_cluster/health/estest HTTP/1.1[\r][\n]"
			"Content-Type: application/json[\r][\n]"
			"User-Agent: Jakarta Commons-HttpClient/3.1[\r][\n]"
			"Host: 192.168.42.24:9200[\r][\n]"
			"Host: 192.168.42.25:9200[\r][\n]"
			"Host: 192.168.42.26:9200[\r][\n]"
			 "HTTP/1.1 200 OK[\r][\n]"	每个节点均返回200OK 正常状态
			 {"cluster_name":"ElasticSearch-Cluster",
			 	"status":"green",		集群为green状态 ，正常状态
			 	"timed_out":false,
			 	"number_of_nodes":3,	所有节点个数
			 	"number_of_data_nodes":3,	数据节点个数
			 	"active_primary_shards":5	物理分片个数
			 	......等
			 }
			 "GET / HTTP/1.1[\r][\n]"
			 "Host: 192.168.42.26:9200[\r][\n]"
			 "HTTP/1.1 200 OK[\r][\n]"
			 "tagline" : "You Know, for Search"[\n]"
			 26返回数据存放在他自己的节点上
		 */

		//3 "HEAD /estest/_mapping/20180123
		/*// 在存放_index的26节点上检查要查询的_index的,
		// _type的_mapping
		//"Host: 192.168.42.26:9200[\r][\n]"
		//"HTTP/1.1 200 OK[\r][\n]"  26 返回查询正常
		*/

		//4 es通过分片进行查询GET /estest/_search_shards
		/*
			Host: 192.168.42.26:9200[\r][\n]"
			"HTTP/1.1 200 OK[\r][\n]"
			Reading from [estest/20180123]  以上检查完毕在分片上开始对_index/_type进行查询
		 */
		//5 获取结构 GET /estest/20180123/_mapping
		/*
		"Host: 192.168.42.26:9200[\r][\n]"
		"HTTP/1.1 200 OK[\r][\n]"
		"{"estest":{"mappings":{"20180123":{
			"properties":{"age":{
				"type":"text",
					"fields":{
						"keyword":{"type":"keyword","ignore_above":256}}},
						"checkintime":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
						"id":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
						"name":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}}}}}"
		 */

		//6 检查查询大小size，同时对所有分片进行查询

		/*
			确认无误  实际集群就是按5个分片进行的存储
		_shards": {
		"total": 5,
				"successful": 5,
				"skipped": 0,
				"failed": 0*/
		/*
		"GET /estest/20180123/_search?size=0 不分页 size=0 &preference=_shards：0    _shard：0 开始对分片进行查询 0为第一个
		 "GET /estest/20180123/_search?size=0&preference=_shards:1 HTTP/1.1[\r][\n]"
		 "GET /estest/20180123/_search?size=0&preference=_shards:2 HTTP/1.1[\r][\n]"
		 "GET /estest/20180123/_search?size=0&preference=_shards:3 HTTP/1.1[\r][\n]"
		 "GET /estest/20180123/_search?size=0&preference=_shards:4 HTTP/1.1[\r][\n]"

		 */

		//7 开始匹配查询条件进行对每一个分片的查询
		/*
			 "POST /estest/20180123/_search?sort=_doc&scroll=5m&size=50&preference=_shards%3A1%7C_local HTTP/1.1[\r][\n]"
		 	"Host: 192.168.42.26:9200[\r][\n]"
		 	"{"query":{"query_string":{"query":"x*"}}}"  查询条件
		 	"HTTP/1.1 200 OK[\r][\n]"
		 */

		//8 对每一个分片均进行完成查询（5个分片个1次），26均返回"HTTP/1.1 200 OK[\r][\n]" 查询完毕
		/*
		数据也就返回了
		 */
		//9 但实际整个过程并没有结束 es还会检查查询后的scroll连接信息
		/*
		"POST /_search/scroll?scroll=5m HTTP/1.1[\r][\n]"
		 */
		//10 删除scorll信息，然后关闭整个task
		/*
		"DELETE /_search/scroll HTTP/1.1[\r][\n]"
		Finished task 4.0 in stage 0.0 (TID 4). 708 bytes result sent to driver
		到这里spark也就完成了整个task的整个过程
		 */

		//从以上的过程的就不难发现，es在存储数据和检索数据的过程中都是依靠着对_index
		//因为使用了负载均衡的分片特性，同时为了查询速度先进行对象检查，然后获得信息
		//再遍历所有分片，直到最后关闭连接的这个过程，还是很有意思的
	}
}
