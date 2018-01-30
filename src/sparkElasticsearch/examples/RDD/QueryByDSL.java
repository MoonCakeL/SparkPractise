package sparkElasticsearch.examples.RDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.Map;

/**
 * 通过使用spark RDD对es进行查询，多半情况下还是使用Spark SQL的比较多，
 * 因为基于sql语法的查询，以及Dataset的列表展示会比json更加有阅读性
 *
 * 而ES对于Spark的 RDD SQL Straming Structured Streaming 都有分别不同的
 * 入库类，但基本都是esRDD() esJsonRDD() saveToEs*()的方法
 * 所以使用不同的Spark技术站中的技术，需要切换不同的JavaEsSpark*的类
 */
public class QueryByDSL {
	private static final SparkConf sparkConf = new SparkConf()
			.setMaster("local")
			.setAppName("QueryByDSL")
			.set("es.nodes","192.168.42.24,192.168.42.25,192.168.42.26")
			.set("es.port","9200");
	private static final JavaSparkContext jsc = new JavaSparkContext(sparkConf);

	public static void main(String[] args){
		//想要在Elasticsearch中检索数据 需要通过_index _type _id来进行

		JavaRDD<Map<String,Object>> esRDD =
				JavaEsSpark.esRDD(
						jsc,
						//_index/_type/id  当然一般来说这个id部分如果是让es自动生成
						//查询时，也是不必须要加的，但_index和_type是"库"和"表"是必须要加的
						"estest/20180123"
				).values();
		//_id的概念就是对一个document的唯一id，可以直接查询这一个文档下的内容
		//但在spark中可以直接对整个_type下的数据进行查询，因为如果没有特殊设计_id
		//使用它查询也是没有意义的
		/*{
			"_index": "estest",
				"_type": "20180123",
				"_id": "AWEix04TP0YuplwDnVY2",
				"_version": 1,
				"found": true,
				"_source": {
			"id": "123456",
					"name": "x1",
					"age": "30",
					"checkintime": "201801231925"
		}
		}*/

		//esRDD.foreach(x -> System.out.println("Json Document value:"+x.values()));
		/*
		Json Document:[123456, x1, 30, 201801231925]
		Json Document:[123456, x1, 30, 201801230000]
		在这里能看到如果我们只拿value部分，是按照这样的数据形式返回的，其实真正返回的结构
		在后台日志中是个Json的结构，是JavaEsSpark帮忙格式化了
		 */

		//esRDD.foreach(x ->
		//		{
		//			for (Map.Entry entry : x.entrySet()){
		//				System.out.println("Json Documnet Field:" + entry.getKey()
		//						+" Json Document value:" +entry.getValue());
		//			}
		//		}
		//);
		/* "{"query":{"match_all":{}}}"跟我们的查询条件在后台自动完成了DSL语法的转义
		Json Documnet Field:id Json Document value:123456
		Json Documnet Field:name Json Document value:x1
		Json Documnet Field:age Json Document value:30
		Json Documnet Field:checkintime Json Document value:201801231925
		Json Documnet Field:id Json Document value:123456
		Json Documnet Field:name Json Document value:x1
		Json Documnet Field:age Json Document value:30
		Json Documnet Field:checkintime Json Document value:201801230000
		 */

		JavaRDD<Map<String,Object>> esRDD2 =
				JavaEsSpark.esRDD(
						jsc,
						//_index/_type/id  当然一般来说这个id部分如果是让es自动生成
						//查询时，也是不必须要加的，但_index和_type是"库"和"表"是必须要加的
						"estest/20180123"
				).values();
		//esRDD2.filter(x -> x.containsValue("201801231925"))
		//		.foreach(x ->
		//				{
		//				for (Map.Entry entry : x.entrySet()){
		//						System.out.println("Json Documnet Field:" + entry.getKey()
		//								+" Json Document value:" +entry.getValue());
		//					}
		//				}
		//		);
		/* 进行内容的过滤，但是这个本身是spark完成对整个es返回结果的过滤，不是es进行的，对于JavaEsSpark的条件参数还需要
		Json Documnet Field:id Json Document value:123456
		Json Documnet Field:name Json Document value:x1
		Json Documnet Field:age Json Document value:30
		Json Documnet Field:checkintime Json Document value:201801231925
		 */

		JavaRDD<Map<String,Object>> esRDD3 =
				JavaEsSpark.esRDD(
						jsc,
						//_index/_type/id  当然一般来说这个id部分如果是让es自动生成
						//查询时，也是不必须要加的，但_index和_type是"库"和"表"是必须要加的
						"estest/20180123",
						"?q=checkintime:201801231925"  //使用这样的写法就是由es
															// 进行的dsl语言解析完成的es查询
				/*
				需要说明的是JavaEsSpark.esRDD的第三个参数只能传入query条件，其它的dsl 类似_source等是不行的
				 */
																										//"{"query":{"query_string":{"query":"checkintime:201801231925"}}}"

				).values();
		esRDD3.foreach(x ->
						{
							for (Map.Entry entry : x.entrySet()){
								System.out.println("Json Documnet Field:" + entry.getKey()
										+" Json Document value:" +entry.getValue());
							}
						}
				);
		/**
		 * 总结，对于Elasticsearch对Spark RDD提供的唯一入口类JavaEsSpark
		 * 中的方法其实并不多，基本就是读和写，而单个读和写支持的功能，也不多
		 * 不能完全向dsl那样做很多的操作
		 *	读
		 	* esRDD()
		 	* esJsonRDD()
		 * 存
		 	* saveToEs()
		 	* saveToEsWithMeta()  mapping
		 	* saveJsonToEs()
		 	* saveJsonByteArrayToEs()
		 */

	}
}
