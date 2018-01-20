package spark.sparkSQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataFrameCreateHive {

	static SparkSession spark = SparkSession.builder()
											.appName("DataFrameCreateHive")
											.master("local")
											.enableHiveSupport() //连接Hive
											.getOrCreate();
	
	public static void main(String[] args) {
		System.setProperty("HADOOP_USER_NAME","hadoop");
		DataFrameDemoHive();

	}

	public static void DataFrameDemoHive(){
		System.setProperty("HADOOP_USER_NAME","hadoop");


		System.out.println();
		String tableName = "dsf.wa_cp_0001";
		Dataset<Row> df = spark.read().table(tableName);
		
		//df.printSchema();
		//对于DataFrame这种“无类型”来说，数据类型对它是无效的,只是记录着内容，“无类型”甚至不区分指令和数据
		/*root
		 |-- column0: integer (nullable = true)
		 |-- column1: string (nullable = true)
		 |-- column2: long (nullable = true)
		 |-- column3: double (nullable = true)
		 |-- column4: float (nullable = true)
		 |-- column5: timestamp (nullable = true)
		 |-- column6: boolean (nullable = true)
		 |-- column7: string (nullable = true)
		 |-- columnHive: string (nullable = true)
		 |-- createDate: string (nullable = true)*/
		
		df.sqlContext().sql("select * from dsf.wa_cp_0001 where limit 10").show();
		//从这里能看出SparkSession确实已经封装了sqlContext()方法		
		Dataset<Row> sqlDF = spark.sql("select * from dsf.wa_cp_0001 where limit 10");
		sqlDF.show();
		
		df.sqlContext().sql("select * from dfs.wa_cp_0001");
	}
}
