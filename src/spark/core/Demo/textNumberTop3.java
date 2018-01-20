package spark.core.Demo;
//文本内容中，取最大的三个数字

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;

public class textNumberTop3 {
	private static SparkSession spark = SparkSession.builder()
			.master("local")
			.appName("textNumberTop3Deom")
			.getOrCreate();

	//private static JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

	public static void main(String[] args){
		textNumberTop3Demo();
	}

	private static void textNumberTop3Demo(){
		JavaRDD<String> textNumberTop3RDD = spark.read()
				.textFile("src/resource/textNumberTop3").javaRDD()
				//sc.textFile("src/resource/textNumberTop3")
				.mapToPair(
						new PairFunction<String, Integer, String>() {
							private static final long serialVersionUID = -784651082944794020L;

							@Override
							public Tuple2<Integer, String> call(String s) throws Exception {
								return new Tuple2<Integer,String>(Integer.valueOf(s),s);
							}
						}
						//降序
				).sortByKey(false).map(
						new Function<Tuple2<Integer, String>, String>() {
							private static final long serialVersionUID = -7886284881430314353L;

							@Override
							public String call(Tuple2<Integer, String> tuple2) throws Exception {
								return tuple2._2;
							}
						}
				);

		List<String> list = textNumberTop3RDD.take(3);

		for (String num : list){
			System.out.println("Number: " + num);
		}
	}

}
