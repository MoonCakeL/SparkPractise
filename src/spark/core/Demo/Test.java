package spark.core.Demo;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class Test
{
	public static void main(String[] args){
		SparkSession spark = SparkSession.builder()
				.master("local").appName("test").getOrCreate();

		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		/*spark.sparkContext().parallelize(Arrays.asList(1,2,3,4),1);

		sc.parallelize()*/

	}
}
