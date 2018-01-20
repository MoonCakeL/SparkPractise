package spark.sparkSQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

public class DatasetCreateHive {

	
	public static class People implements Serializable{

		private int id;
		private String name;
		private int age;
		private String sex;
		
		public People(){

		}

		public String getSex() {
			return this.sex;
		}

		public void setID(int id){
			this.id = id;
		}
		
		public void setName(String name){
			this.name = name;
		}
		
		public void setAge(int age){
			this.age = age;
		}

		public int getId() {
			return this.id;
		}
		
		public void setSex(String sex){
			this.sex = sex;
		}
		
		public String getName(){
			return this.name;
		}
		
		public int getAge(){
			return this.age;
		}
	}

	public static class People2 implements Serializable{

		private String name;
		private int age;


		public People2() {
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}
	}
	public static void main(String[] args){
		
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		
		SparkSession spark = SparkSession.builder()
										.appName("DatasetCreateHive")
										.master("local")
										.enableHiveSupport()
										.getOrCreate();
		//spark.sql("create table if not exists default.people (id STRING,name STRING,age INT,sex STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
		//spark.sql("load data inpath '/people.txt' into table people");
		
		//create dataframe
		//Dataset<Row> df = spark.sql("select * from people");
		
		//df.printSchema();

		//df.show();
		/*root
		 |-- id: string (nullable = true)
		 |-- name: string (nullable = true)
		 |-- age: integer (nullable = true)
		 |-- sex: string (nullable = true)*/
		
		//通过Encoders实现自定义的序列化格式，这里通过Encoders.bean(Java beans)反射推断模式Schema（但需要提前知道数据结构，也就是在业务结构等都定下来的前提）
	    //的方法可以获取到数据类型，变成“强类型”
	    //Encoder<People> peopleEncoder = Encoders.bean(People.class);
	    //DataFrames可以通过已经预备的javabean，基于“名字（字段）”的映射，来转换成一个Dateset
	    //Dataset<People> personjavaBeanDS = spark.sql("select * from people").as(personEncoder);
		
	    //personjavaBeanDS.show();
	    
	    //personjavaBeanDS.printSchema();


	    //通过实现Encoders来指定数据类型，来实现“强类型” 
	    // Encoders for most common types are provided in class Encoders
	    /*Encoder<Integer> integerEncoder = Encoders.INT();
	    Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
	    Dataset<Integer> transformedDS = primitiveDS.map(
	        (MapFunction<Integer, Integer>) value -> value + 1,
	        integerEncoder);*/
	    //transformedDS.show();

	    //Dataset<Row> transformedDS2 = spark.sql("select id,name,age from people where age >=30");

		//transformedDS2.show();
		//直接将结果存入Hive，通过sparksql自带的推断模式完成表创建
		//transformedDS2.write().format("parquet").mode("ignore").saveAsTable("people2");
	/*	ParquetWriteSupport: Initialized Parquet WriteSupport with Catalyst schema:
		{
			"type" : "struct",
				"fields" : [ {
			"name" : "id",
					"type" : "string",
					"nullable" : true,
					"metadata" : { }
		}, {
			"name" : "name",
					"type" : "string",
					"nullable" : true,
					"metadata" : { }
		}, {
			"name" : "age",
					"type" : "integer",
					"nullable" : false,
					"metadata" : { }
		} ]
		}
		and corresponding Parquet message type:
		message spark_schema {
			optional binary id (UTF8);
			optional binary name (UTF8);
			required int32 age;
		}


		18/01/08 11:17:40 INFO CodecPool: Got brand-new compressor [.snappy]//默认压缩
		18/01/08 11:17:40 INFO InternalParquetRecordWriter: Flushing mem columnStore to file. allocated memory: 29
		18/01/08 11:17:41 INFO FileOutputCommitter:
		Saved output of task 'attempt_20180108111740_0004_m_000000_0'
		to hdfs://192.168.42.24:9000/hivedata/people2/_temporary/0/task_20180108111740_0004_m_000000*/

		/*Save Modes （保存模式）
		Save operations （保存操作）可以选择使用 SaveMode ,
		它指定如何处理现有数据如果存在的话. 重要的是要意识到,
		这些 save modes （保存模式）不使用任何 locking （锁定）并且不是 atomic （原子）.
		另外, 当执行 Overwrite 时, 数据将在新数据写出之前被删除.

		Scala/Java	Any Language	Meaning
		1.SaveMode.ErrorIfExists (default)	"error" (default)
			将 DataFrame 保存到 data source （数据源）时, 如果数据已经存在, 则会抛出异常.

		2.SaveMode.Append	"append"
			将 DataFrame 保存到 data source （数据源）时, 如果 data/table 已存在,
			则 DataFrame 的内容将被 append （附加）到现有数据中.
		3.SaveMode.Overwrite	"overwrite"
			Overwrite mode （覆盖模式）意味着将 DataFrame 保存到 data source （数据源）时,
			如果 data/table 已经存在,
			则预期 DataFrame 的内容将 overwritten （覆盖）现有数据.

		4.SaveMode.Ignore	"ignore"
			Ignore mode （忽略模式）意味着当将 DataFrame 保存到 data source （数据源）时,
			如果数据已经存在, 则保存操作预期不会保存 DataFrame 的内容,
			并且不更改现有数据. 这与 SQL 中的 CREATE TABLE IF NOT EXISTS 类似.*/


			/*
			people:desc
				+---+-------+---+---+
				| id|   name|age|sex|
				+---+-------+---+---+
				|  1|Michael| 29| ��|
				|  2|   Andy| 30|  Ů|
				|  3| Justin| 19| ��|
				+---+-------+---+---+
			people2:decs
				+---+----+---+
				| id|name|age|
				+---+----+---+
				|  2|Andy| 30|
				+---+----+---+*/

		Dataset<Row> df2 = spark.sql("select p1.id," +
				"p1.name," +
				"p1.age" +
				" from people p1,people2 p2" +
				" where p1.id <> p2.id");

		df2.show();
			/*people3:desc
				+---+-------+---+
				| id|   name|age|
				+---+-------+---+
				|  1|Michael| 29|
				|  3| Justin| 19|
				+---+-------+---+*/

			//这里可以再次使用sql(内置函数)过滤，然后直接存储按照age存储为分区结构
		/*df2.select("name","age").write()
				.format("parquet")
				.partitionBy("age")
				.mode("append")
				.saveAsTable("people3");*/

		/*Dataset<Row> df3 = spark.sql("select * from people3");

		df3.printSchema();
		df3.show();*/

		/*spark-sql> desc people3;

		name    string NULL
		age     int     NULL
		# Partition Information
		# col_name      data_type       comment
		age     int     NULL
		Time taken: 0.078 seconds, Fetched 5 row(s)
		18/01/08 11:50:*/

		//通过反射方式创建schema，按照指定分区插入数据
		/*People2 people2 = new People2();
		people2.setName("Tom");
		people2.setAge(20);*/
		/*
		这里添加两个字段，重新定义javabean，并使用spark dataset的自定义序列化方法Encoder

		 */
		/*Encoder<People2> people2Encoder = Encoders.bean(People2.class);

		Dataset<People2> addData = spark.createDataset(
				Collections.singletonList(people2),
				people2Encoder
		);*/

		//addData.show();
		//看样子不能自动增加列，"哈哈想多了"
		//The column number of the existing table default.people3(struct<name:string,age:int>)
		// doesn't match the data schema(struct<age:int,id:int,name:string>);
		//addData.write().partitionBy("age").mode("append").save("people3");

		//直接指定表名完成Dataset的创建
		Dataset<Row> peopleDs = spark.table("people3");

		peopleDs.show();
		spark.close();

	}
}
