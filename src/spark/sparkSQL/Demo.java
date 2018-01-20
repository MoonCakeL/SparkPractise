package spark.sparkSQL;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.LinkedList;

public class Demo {
	static SparkSession spark = SparkSession
								.builder()
								.appName("SparkSQL Demo")
								.master("local")
								.getOrCreate();
	static JavaRDD<Row> javaRDD = spark.read().csv("src/resources/201404bus.csv").toJavaRDD();
	static Dataset<Row> df0 = spark.read().csv("src/resources/201404bus.csv");
	
	public static void main(String[] args) {
		
		//df.show();
		//df0.printSchema();
		
		String schemaString = "CARDID LINEID BEGINTIME ENDTIME STATIONFROM STATIONTO BUSID";
		
		LinkedList<StructField> fields = new LinkedList<StructField>();
		
		for(String fieldName : schemaString.split(" ")){
			
			StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			
			fields.add(field);
		}
		
		StructType schema = DataTypes.createStructType(fields);
		
		Dataset<Row> df = spark.createDataFrame(javaRDD, schema);
		
		//df.show();
		/*df0.printSchema();
		df.printSchema();*/
		df.createOrReplaceTempView("bus");
		
		Dataset<Row> teenagersDF = spark.sql("SELECT * FROM bus WHERE BUSID=00017499");
		
		//teenagersDF.show();
		//从row对象数组中通过getString[0]下标取数据
		Encoder<String> stringEncoder = Encoders.STRING();
	    Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
	            (MapFunction<Row, String>) row -> "CARDID: " + row.getString(0),
	            stringEncoder);
	    //teenagerNamesByIndexDF.show();
		
	    Dataset<String> teenagerNamesByNameDF = teenagersDF.map(
	    		(MapFunction<Row,String>) row -> "LINEID:" + row.<String>getAs("LINEID"),
	    		stringEncoder);
		
	    //teenagerNamesByNameDF.show();
/*		Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
				new MapFunction<Row,String>() {
					public String call(Row row){
						return	row.getString(0).toString();
					}				
				}); */
	    
	    Dataset<Row> groupDF = spark.sql("select BUSID,count(1) from bus group by BUSID having count(1) > 100");
	    
	    //groupDF.show();
	    groupDF.createOrReplaceTempView("maxbus");
	    
	    Dataset<Row> maxTimeDF = spark.sql("select max(a.BEGINTIME) maxbegin,max(a.ENDTIME) maxend from bus a,maxbus b where b.BUSID=a.BUSID");
	    
	    maxTimeDF.show();
	}

	public void BasicDataSourcecvs(){
		Dataset<Row> peopleDFCsv = spark.read().format("csv")
				.option("sep", ";") //设置分隔字符
				.option("inferSchema", "true") //推断Schema
				.option("header", "true") //使用csv头部第一行推断Schema
				.load("src/resources/people.csv");

		peopleDFCsv.show();
	}
	
}
