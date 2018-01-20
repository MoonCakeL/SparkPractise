/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.examples.sql;

// $example on:programmatic_schema$
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.col;

// $example off:programmatic_schema$
// $example on:create_ds$
// $example off:create_ds$
// $example on:schema_inferring$
// $example on:programmatic_schema$
// $example off:programmatic_schema$
// $example on:create_ds$
// $example on:create_df$
// $example on:run_sql$
// $example on:programmatic_schema$
// $example off:programmatic_schema$
// $example off:create_df$
// $example off:run_sql$
// $example off:create_ds$
// $example off:schema_inferring$
// $example on:init_session$
// $example off:init_session$
// $example on:programmatic_schema$
// $example off:programmatic_schema$
// $example on:untyped_ops$
// col("...") is preferable to df.col("...")
// $example off:untyped_ops$

public class JavaSparkSQLExample {
  // $example on:create_ds$
  public static class Person implements Serializable {
    private String name;
    private int age;

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
  // $example off:create_ds$

  public static void main(String[] args) {
    // $example on:init_session$
    SparkSession spark = SparkSession
      .builder()
      .appName("Java Spark SQL basic example")
      //.config("spark.some.config.option", "some-value")
      .master("local")
      .getOrCreate();
    // $example off:init_session$

    /*创建一个DataFrame，在spark2.0中Dateset包含了DataFrame的功能，所以创建使用Dataset<Row>来接收
	而DataFrame代表为Dataset<Row>的子集，Row也是分布式数据集，同时也是“一行数据”的对象，本质其实是一个一维数组
	可以使用get下标的方式，操作数组的方法来操作Row对象*/
    //runBasicDataFrameExample(spark);
    
    //Dataset的创建反射转换
    //runDatasetCreationExample(spark);
    
    //完整的一个反射转换RDD创建Dataset过程
    //runInferSchemaExample(spark);
    runProgrammaticSchemaExample(spark);

    spark.stop();
  }

  private static void runBasicDataFrameExample(SparkSession spark) throws AnalysisException {
    // $example on:create_df$
	//通过一个json创建DataFrame，用Dataset<Row>对象接收
    Dataset<Row> df = spark.read().json("src/resources/people.json");

    // Displays the content of the DataFrame to stdout
    df.show();
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // $example off:create_df$

    // $example on:untyped_ops$
    // Print the schema in a tree format
    //打印模式：查看DataFrame中的元数据，DataFrame本身是“无类型”的，只是知道字段，不知道类型，而DataSet都知道因为可以
    //通过Encoder实现自定义的序列化格式如:Person.class (implements Serializable 序列化必须继承Serializable接口)
    df.printSchema();
    // root
    // |-- age: long (nullable = true)
    // |-- name: string (nullable = true)
    
    //以下为DataFrame为DataSet[Row]，提供可以执行一些非强制类型的转换
    // Select only the "name" column
    //df.select("name").show();
    // +-------+
    // |   name|
    // +-------+
    // |Michael|
    // |   Andy|
    // | Justin|
    // +-------+
    
    //不确定类型情况下的+1操作
    // Select everybody, but increment the age by 1
   // df.select(col("name"), col("age").plus(1)).show();
    // +-------+---------+
    // |   name|(age + 1)|
    // +-------+---------+
    // |Michael|     null|
    // |   Andy|       31|
    // | Justin|       20|
    // +-------+---------+

    //col转换strping类型情况下的过滤操作
    // Select people older than 21
    df.filter(col("age").gt(21)).show();
    // +---+----+
    // |age|name|
    // +---+----+
    // | 30|Andy|
    // +---+----+

    //不确定类型情况下的分组统计操作
    //DataFrame除了简单的列引用和表达式之外，也有丰富的函数库，包括string操作，date算数，具体需要看DataFram函数指南
    // Count people by age
    df.groupBy("age").count().show();
    // +----+-----+
    // | age|count|
    // +----+-----+
    // |  19|    1|
    // |null|    1|
    // |  30|    1|
    // +----+-----+
    // $example off:untyped_ops$
    
    //SparkSession的sql函数可以让程序运行sql查询，并将结果作为DataFrame返回。
    // $example on:run_sql$
    // Register the DataFrame as a SQL temporary view
    //创建临时试图，非全局的，当前session消失而消失
    df.createOrReplaceTempView("people");

    Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
    sqlDF.show();
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // $example off:run_sql$

    // $example on:global_temp_view$
    // Register the DataFrame as a global temporary view
    //全局临时视图是session级别的，会随着session消失而消失，，如果想一个临时试图在
    //所有session中相互传递并可用，直到spark应用退出，可以建立一个全局的临时试视图
    //全局的临时试图存放在系统数据库global_temp中，使用的时候需要加上库名引用它。
    df.createGlobalTempView("people");

    // Global temporary view is tied to a system preserved database `global_temp`
    spark.sql("SELECT * FROM global_temp.people").show();
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+

    // Global temporary view is cross-session
    spark.newSession().sql("SELECT * FROM global_temp.people").show();
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // $example off:global_temp_view$
  }

  private static void runDatasetCreationExample(SparkSession spark) {
    // $example on:create_ds$
    // Create an instance of a Bean class
    Person person = new Person();
    person.setName("Andy");
    person.setAge(32);

    // Encoders are created for Java beans
    //通过Encoders实现自定义的序列化格式，这里通过Encoders.bean(Java beans)
    //的方法可以获取到数据类型，变成“强类型”
    Encoder<Person> personEncoder = Encoders.bean(Person.class);
    //Collections.singletonList()用来生成只读的，单一元素List
    Dataset<Person> javaBeanDS = spark.createDataset(
      Collections.singletonList(person),
      personEncoder
    );
    javaBeanDS.show();
    // +---+----+
    // |age|name|
    // +---+----+
    // | 32|Andy|
    // +---+----+
    
    //通过实现Encoders来指定数据类型，来实现“强类型”
    // Encoders for most common types are provided in class Encoders
    Encoder<Integer> integerEncoder = Encoders.INT();
    Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
    Dataset<Integer> transformedDS = primitiveDS.map(
        (MapFunction<Integer, Integer>) value -> value + 1,
        integerEncoder);
    transformedDS.collect(); // Returns [2, 3, 4]

    //DataFrames可以通过已经预备的javabean，基于“名字（字段）”的映射，来转换成一个Dateset
    // DataFrames can be converted to a Dataset by providing a class. Mapping based on name
    String path = "src/main/resources/people.json";
    Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
    peopleDS.show();
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // $example off:create_ds$
  }

  private static void runInferSchemaExample(SparkSession spark) {
    // $example on:schema_inferring$
    // Create an RDD of Person objects from a text file
	//完成的Dataset通过一个RDD的反射推理schema的转换过程
	//先创建一个有数据的JavaRDD
    JavaRDD<Person> peopleRDD = spark.read()
      .textFile("src/resources/people.txt")
      .javaRDD()
      .map(line -> {
        String[] parts = line.split(",");
        Person person = new Person();
        person.setName(parts[0]);
        person.setAge(Integer.parseInt(parts[1].trim()));
        return person;
      });

    // Apply a schema to an RDD of JavaBeans to get a DataFrame
    //创建DataFrame
    Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
    // Register the DataFrame as a temporary view
    peopleDF.createOrReplaceTempView("people");

    // SQL statements can be run by using the sql methods provided by spark
    Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");

    // The columns of a row in the result can be accessed by field index
    //将dataframe中的row对象map转换，同时完成数据类型，从row对象数组中通过getString[0]下标取数据
    Encoder<String> stringEncoder = Encoders.STRING();
    Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
        (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
        stringEncoder);
    teenagerNamesByIndexDF.show();
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+

    // or by field name
    //通过fieldname取数据
    Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(
        (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
        stringEncoder);
    teenagerNamesByFieldDF.show();
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+
    // $example off:schema_inferring$
  }

  private static void runProgrammaticSchemaExample(SparkSession spark) {
    // $example on:programmatic_schema$
    // Create an RDD
    JavaRDD<String> peopleRDD = spark.sparkContext()
      .textFile("src/resource/people.txt", 1)
      .toJavaRDD();
   System.out.println(peopleRDD.collect());
    
    // The schema is encoded in a string
    String schemaString = "id name age sex";

    // Generate the schema based on the string of schema
    List<StructField> fields = new ArrayList<>();
    for (String fieldName : schemaString.split(" ")) {
      StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
      fields.add(field);
    }
    StructType schema = DataTypes.createStructType(fields);

    // Convert records of the RDD (people) to Rows
    JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
      String[] attributes = record.split(",");
      return RowFactory.create(attributes[0].trim(), attributes[1],attributes[2].trim(),attributes[3]);
    });

    // Apply the schema to the RDD
    Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

    // Creates a temporary view using the DataFrame
    peopleDataFrame.createOrReplaceTempView("people");

    // SQL can be run over a temporary view created using DataFrames
    Dataset<Row> results = spark.sql("SELECT name FROM people");

    // The results of SQL queries are DataFrames and support all the normal RDD operations
    // The columns of a row in the result can be accessed by field index or by field name
    Dataset<String> namesDS = results.map(
        (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
        Encoders.STRING());
    namesDS.show();
    // +-------------+
    // |        value|
    // +-------------+
    // |Name: Michael|
    // |   Name: Andy|
    // | Name: Justin|
    // +-------------+
    // $example off:programmatic_schema$
  }
}
