package spark.spark2upgrade;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.io.Serializable;

/**
 * onType的强类型操作，Dataset
 * 其实基本上强类型的操作主要是可以对代码提升安全性
 * ，不用等到代码运行的时候才会出现类型错误这样的问题，
 * 类似sql来说，一个sql语句编写好了之后，只有在运行的
 * 时候才能看到的类型是不正确的问题。
 *
 * 所以其实这样无论是untype还是ontype都是可以使用
 * sql来进行完成的，但是还是那句话sql的语句只有在，
 * 执行的时候才会暴露出类型正确的问题。
 *
 * SparkSQL把给Dataset的API 分成无类型和强类型的目的
 * 主要是无类型值对DataFrame的
 * 而Dataset的API都是强类型，而且某种意义上对每一个API
 * 提出来之后，不再sql中实现，变会有类型上的提示，将会编译
 * 不过去，编译器就会报错。
 *
 * 如：你对一个string的元素做强类型的api操作，那么编译器便
 * 会直接报错，编译不通过。
 *
 *  DataFrames/Datasets 应用各种操作 - 从 untyped （无类型）， SQL-like operations （类似 SQL 的操作）
 *  （例如 select ， where ， groupBy ）
 *  到 typed RDD-like operations （类型化的类似 RDD 的操作）（
 *  例如 map ，filter ， flatMap ）,
 *  对于ontype的类型可以使用case class进行反射（注意如果是case class（javabean）需要写在方法最上面）
 *  或者变成模式写schema
 *  有关详细信息，请参阅 SQL 编程指南 。
 *
 */
public class TypeAction {

	public static class employeeClass implements Serializable{
		private String name;
		private Long age;
		private Long depId;
		private String gender;
		private Long salary;

		public employeeClass() {
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Long getAge() {
			return age;
		}

		public void setAge(Long age) {
			this.age = age;
		}

		public Long getDepId() {
			return depId;
		}

		public void setDepId(Long depId) {
			this.depId = depId;
		}

		public String getGender() {
			return gender;
		}

		public void setGender(String gender) {
			this.gender = gender;
		}

		public Long getSalary() {
			return salary;
		}

		public void setSalary(Long salary) {
			this.salary = salary;
		}
	}


	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
				.appName("UnTypedAction")
				.master("local").getOrCreate();
		//员工记录表DataFrame
		Dataset<Row> employee = spark.read().json("src/resource/employee.json");

		//From DataFrame Create Dataset
		Encoder<employeeClass> employeeEncoders=Encoders.bean(employeeClass.class);
		Dataset<employeeClass> employeeDS =employee.as(employeeEncoders);
		employeeDS.show();

		//部门表
		//Dataset<Row> department = spark.read().json("src/resource/department.json");

		/*
		coalesce和repartition操作
			 都是用来重新定义分区的
			 区别在于：coalesce，只能用于减少分区数量，而且可以选择不发生shuffle
			repartiton，可以增加分区，也可以减少分区，必须会发生shuffle，相当于是进行了一次重分区操作
			*/

		//先打印一下employee的默认分区情况
		//System.out.println("default ->"+employeeDS.rdd().partitions().length);
		/*
		default ->1
		 */
		//对employee重新分区，在打印一下分区情况
		//System.out.println("repartition ->"+employeeDS.repartition(7).rdd().partitions().length);
		/*
		Planning scan with bin packing, max size: 4194834 bytes,
		open cost is considered as scanning 4194304 bytes.
		repartition->7
		*/
		//对employee减少分区，在打印一下分区情况
		//System.out.println("coalesce ->"+employeeDS.coalesce(3).rdd().partitions().length);
		/*
		Planning scan with bin packing, max size: 4194834 bytes,
		open cost is considered as scanning 4194304 bytes.
		coalesce ->1
		 */

		/*
		distinct和dropDuplicates
		都是用来进行去重的
		区别:
		distinct：
			是根据每一条数据(一整行中所有字段)，进行完整内容的比对和去重，
			两行字段一摸一样就会去重。

		dropDuplicates：
			可以根据指定的一个或者多个字段进行去重，如果只是字段相同，但其它字段不同
			也会被去重

		employee原始数据：
			{"name": "Leo", "age": 25, "depId": 1, "gender": "male", "salary": 20000}
			{"name": "Marry", "age": 30, "depId": 2, "gender": "female", "salary": 25000}
			{"name": "Marry", "age": 30, "depId": 2, "gender": "female", "salary": 25000}
			{"name": "Jack", "age": 35, "depId": 1, "gender": "male", "salary": 15000}
			{"name": "Tom", "age": 42, "depId": 3, "gender": "male", "salary": 18000}
			{"name": "Kattie", "age": 21, "depId": 3, "gender": "female", "salary": 21000}
			{"name": "Jen", "age": 30, "depId": 2, "gender": "female", "salary": 28000}
			{"name": "Jen", "age": 19, "depId": 2, "gender": "female", "salary": 8000}
		 */
		//对employee使用distinct并打印结果
		//employeeDS.distinct().show();
				/*
				只有整行一摸一样的"Marry"被去重掉了
				+---+-----+------+------+------+
				|age|depId|gender|  name|salary|
				+---+-----+------+------+------+
				| 30|    2|female|   Jen| 28000|
				| 35|    1|  male|  Jack| 15000|
				| 21|    3|female|Kattie| 21000|
				| 30|    2|female| Marry| 25000|
				| 19|    2|female|   Jen|  8000|
				| 25|    1|  male|   Leo| 20000|
				| 42|    3|  male|   Tom| 18000|
				+---+-----+------+------+------+*/
		//employee使用dropDuplicates并打印结果,多个字段可以传入数组或者list进行去重
		//employeeDS.dropDuplicates("name").show();
				/*
				名字一样但工资不一样的"Jen"被去重掉了，因为基于单个name字段
				+---+-----+------+------+------+
				|age|depId|gender|  name|salary|
				+---+-----+------+------+------+
				| 35|    1|  male|  Jack| 15000|
				| 42|    3|  male|   Tom| 18000|
				| 30|    2|female|   Jen| 28000|
				| 30|    2|female| Marry| 25000|
				| 21|    3|female|Kattie| 21000|
				| 25|    1|  male|   Leo| 20000|
				+---+-----+------+------+------+*/

		// except：获取在当前dataset中有，但是在另外一个dataset中没有的元素
		// filter：根据我们自己的逻辑，如果返回true，那么就保留该元素，否则就过滤掉该元素
		// intersect：获取两个数据集的交集

		// map：将数据集中的每条数据都做一个映射，返回一条新数据
		Encoder<Long> longEncoder = Encoders.LONG();
		Encoder<String> stringEncoder = Encoders.STRING();
		employeeDS.map(
				(MapFunction<employeeClass, String>)x -> x.name, stringEncoder

		).show();

		// flatMap：数据集中的每条数据都可以返回多条数据


		// mapPartitions：一次性对一个partition中的数据进行处理（*注意基于分区处理，内部输入输出都是一个迭代器）

	}
}
