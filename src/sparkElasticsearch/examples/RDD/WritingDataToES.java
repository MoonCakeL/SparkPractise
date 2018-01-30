package sparkElasticsearch.examples.RDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.spark_project.guava.collect.ImmutableList;
import org.spark_project.guava.collect.ImmutableMap;

import java.util.Map;

public class WritingDataToES {
	public static void main(String[] args){
		SparkConf sparkConf = new SparkConf()
				.setMaster("local")
				.setAppName("Writing Data To ES")
				.set("es.nodes","192.168.42.24,192.168.42.25,192.168.42.26")
				//.set("es.nodes","192.168.42.29,192.168.42.30,192.168.42.31")
				.set("es.port","9200");

		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		Map<String,Object> numbers = ImmutableMap.of("one", 1, "two", 2);
		Map<String,Object> airports = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran");

		JavaRDD<Map<String,Object>> javaRDD =
				jsc.parallelize(ImmutableList.of(numbers,airports));

		JavaEsSpark.saveToEs(javaRDD,"sparktest/docs");
		/*
		"hits": [
		{
			"_index": "sparktest",
				"_type": "docs",
				"_id": "AWEn-W5qUMEjV-jeRISu",
				"_score": 1,
				"_source": {
			"one": 1,
					"two": 2
		}
		},
		{
			"_index": "sparktest",
				"_type": "docs",
				"_id": "AWEn-W5qUMEjV-jeRISv",
				"_score": 1,
				"_source": {
			"OTP": "Otopeni",
					"SFO": "San Fran"
		}
		}
    ]
		*/
	}
}
