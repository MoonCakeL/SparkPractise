package spark.spark2upgrade.functions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * 聚合函数	aggregate function:

 approxCountDistinct, avg, count, countDistinct, first, last, max, mean, min, sum, sumDistinct

 */

public class AggFunctions {

	static  SparkSession spark = SparkSession.builder()
			.appName("AggFunctions")
			.master("local").getOrCreate();

	static JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

	public static void main(String[] args) throws Exception{
		Agg();

	}

	public static void Agg() throws Exception{


		List<String> list =Arrays.asList(
				"100,张三",
				"110,张三",
				"200,李四",
				"210,李四");

		StructField[] fields= {
			DataTypes.createStructField("score",DataTypes.IntegerType,true),
			DataTypes.createStructField("name", DataTypes.StringType,true)
		};

		StructType schema = DataTypes.createStructType(fields);

		JavaRDD<Row> javaRowRDD = sc.parallelize(list).map(
				new Function<String, Row>() {
					private static final long serialVersionUID = -8387453831778856877L;

					@Override
					public Row call(String s) {

						LinkedList<Object> dataList = new LinkedList<>();

						String[] arrays = s.split(",");

						dataList.add(Integer.valueOf(arrays[0]));
						dataList.add(arrays[1].toString());

						return RowFactory.create(dataList.toArray());
					}
				}
		);

		Dataset<Row> DF = spark.createDataFrame(javaRowRDD,schema).cache();
		DF.show();

		DF.createOrReplaceTempView("person");

		Dataset<Row> aggSumDF = spark.sql(
				"select name,sum(score) " +
						"from person" +
						" group by name");
		aggSumDF.show();

		Dataset<Row> aggAvgDF = spark.sql("select name,avg(score) avgscore" +
				" from person " +
				"group by name " +
				"order by avgscore ");
		aggAvgDF.show();

		Dataset<Row> aggFirstDF = spark.sql("select name,first(score) " +
				"from person " +
				"group by name");
		aggFirstDF.show();

		Dataset<Row> aggDistinctDF = spark.sql(
				"select distinct name " +
						"from person"
		);
		aggDistinctDF.show();

		Dataset<Row> aggMaxMinDF = spark.sql(
				"select name,max(score),min(score) " +
						"from person " +
						"group by name"
		);
		aggMaxMinDF.show();
	}
}
