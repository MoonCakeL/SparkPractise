package spark.core.Demo;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class RDDDemo {
	static SparkSession spark = SparkSession.builder()
								.appName("JavaRDDDemo")
								.master("local")
								.getOrCreate();
	static JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

	public static void main(String[] args){
		//javaRDD();
		javaRDDSum();
	}

	public static void javaRDD(){

		JavaRDD<String> javaRDD = sc.parallelize(Arrays.asList("a","b","c"));

		List<String> listRDD= javaRDD.collect();

		for (String rdd : listRDD){

			System.out.println("输出RDD：" + rdd);
		}

	}

	public static void javaRDDSum(){
		Integer javaRDDSum = sc.parallelize(Arrays.asList(11,22,12)).reduce(
				new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer integer, Integer integer2) throws Exception {
						return integer + integer2;
					}
				}
		);
			System.out.println("SUM: " + javaRDDSum);
	}
}
