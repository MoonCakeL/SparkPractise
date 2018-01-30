package spark.core.Demo;
//实现一个二次排序的功能
//如 第一列如果相同 就 对第二列进行排序
//再spark中进行二次排序，首先需要自己定义一个funtion的自定义排序key
//需要实现"scala.math"中提供的，Ordered这个接口以及"scala"中提供的序列化接口Serializable
//SecondarySortKey.java再中进行实现和描述


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;
import scala.Tuple2;

public class SecondarySort implements Serializable {
	private static final long serialVersionUID = 1L;
	private static SparkSession spark = SparkSession.builder()
			.appName("Secondary")
			.master("local")
			.getOrCreate()
			;

	private static JavaSparkContext sc =
			new JavaSparkContext(spark.sparkContext());
					//.getConf()
					//.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
					//.registerKryoClasses(new Class[]{SecondarySortKey.class});

	public static void main(String[] args){
		SecondaryBySort();
	}

	/**
	 * 二次排序
	 */
	private static void SecondaryBySort(){
		JavaRDD<String> javaRDD = sc.textFile("src/resource/secondaryWord");

		JavaPairRDD<SecondarySortKey,String> pairSortRDD = javaRDD.mapToPair(
				//出来的 String, SecondarySortKey，进去的String
				new PairFunction<String, SecondarySortKey, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					//出来的Tuple2<SecondarySortKey，进去的String s
					public Tuple2<SecondarySortKey, String> call(String s) throws Exception {
						//文本中内容为
						/*1,5
						2,4
						3,6
						1,3
						2,1*/
						//1.先按照","完成每一行中，每一列进行切分，完成每行中的两列
						//同时JavaPairRDD会自动按照内容中的换行来完成每一行的识别
						String[] sLine = s.split(",");

						//2.按照切分完毕形成first key和second key数组下标传递到排序的key算法中
						//原始RDD为String，判断key算法中依据integer进行判断所以完成一次类型转换
						SecondarySortKey sortKey = new SecondarySortKey(
							Integer.valueOf(sLine[0]),
							Integer.valueOf(sLine[1]));

						//3.完成排序先返回排序中的key以及排序完毕的数据
						return new Tuple2<SecondarySortKey,String>(
								sortKey,s
						);
					}
				}
		).sortByKey();

		pairSortRDD.foreach(new VoidFunction<Tuple2<SecondarySortKey, String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<SecondarySortKey, String> s) throws Exception {
				System.out.println("SecondarySortKey: " + s._1 + " | Word: " + s._2);
				System.out.println("Word: " + s._2);
			}
		});
		sc.close();
	}
}
