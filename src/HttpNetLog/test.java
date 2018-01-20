package HttpNetLog;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class test {
	public static void main(String[] args){


		SparkSession spark = SparkSession.builder().enableHiveSupport()
				.master("local").getOrCreate();

		//Dataset<Row> rowDataset = spark.read().table("httpnetlogdb.httpnetlog");

		Dataset<Row> rowDataset =
				spark.sql("select * from httpnetlogdb.httpnetlog");

		System.out.println("++++++"+rowDataset.count()+"++++++");

		rowDataset.createOrReplaceTempView("tmp");
		spark.sql("select BeginTime,EndTime,MSISDN,SourceIP,SiteName,create_date from tmp where " +
				"MSISDN is not null and BeginTime is not null " +
				"and EndTime is not null " +
				" and SourceIP is not null and create_date='20180119'")
				/*select("BeginTime,EndTime,MSISDN")
				.filter("MSISDN='15287134073'")
				.filter("create_date='20180119'")*/
				.show();

	}
}
