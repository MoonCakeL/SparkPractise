package spark.spark2upgrade;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

//坑爹的funcitions，肯定是spark内部还没有完整的继承所有函数的坑
import static org.apache.spark.sql.functions.*;

public class UnTypedAction {

	public static void main(String[] args){
		SparkSession spark = SparkSession.builder()
				.appName("UnTypedAction")
				.master("local").getOrCreate();
		//员工记录表
		Dataset<Row> employee = spark.read().json("src/resource/employee.json");
		//部门表
		Dataset<Row> department = spark.read().json("src/resource/department.json");
/*
		A more concrete example in Scala:


		// To create Dataset[Row] using SparkSession
		val people = spark.read.parquet("...")
		val department = spark.read.parquet("...")

		people.filter("age > 30")
				.join(department, people("deptId") === department("id"))
				.groupBy(department("name"), people("gender"))
				.agg(avg(people("salary")), max(people("age")))

		and in Java:


		// To create Dataset<Row> using SparkSession
		Dataset<Row> people = spark.read().parquet("...");
		Dataset<Row> department = spark.read().parquet("...");

		people.filter(people.col("age").gt(30))
				.join(department, people.col("deptId").equalTo(department.col("id")))
				.groupBy(department.col("name"), people.col("gender"))
				.agg(avg(people.col("salary")), max(people.col("age")));*/



		/*需求：
			1.过滤员工表年龄大于20；
			2.员工记录表和部门表通过，部门id关联
			3.根据部门名称和员工性别进行分组
			4.最后聚合取平均值
			5.打印结果
			*/
		employee.filter("age >20")
				.join(department,employee.col("depId").equalTo(department.col("id")))
				.groupBy(department.col("name"),employee.col("gender"))
				//聚合算法都是在分组之后进行的所以要在groupby后面
				//这部分有些奇怪用到聚合函数是总是找不到里面的方法原因尚且不明以后再说吧20180115 18：06
				//.agg(avg)
				//.avg(String.valueOf(employee.col("salary")))
				//终于找到原因了，因为看官方api就是这个functions的包，但是导在class上面就是不能用
				//所以说agg只是调用聚合的开始的方法，要在里面写聚合函数，聚合函数又是在functions包里面的
				//真是有毒，在scala代码里看到是现在静态类里面才导入的包才想到，干脆直接从包里面引用方法就没错了
				//真是有毒，但新的问题又出现了，不可能每次调用函数就直接写包名字吧？？？。。。。也许是什么冲突了
				// 201801151913
				.agg(
						/*org.apache.spark.sql.functions.avg(employee.col("salary")),
						org.apache.spark.sql.functions.max(employee.col("salary"))*/
						//最后终于找到原因了
						//原来这个function的包不是静态的
						//需要"import static org.apache.spark.sql.functions.*;"
						//带上"static"静态参数才可以
						//import static是java1.5的新特性
						//而这里的目的应该还是方法名字冲突了，
						// 这个包中的avg等方法是在org.apache.spark.sql的functions.class中的
						//而上面如果不静态引入这个包一样可以使用avg方法，但它是Dataset中的方法
						//是在org.apache.spark.sql的RelationalGroupedDataset.class中的
						//dataset的方法不能直接放在agg聚合函数中
						//这也是在官网的例子中发现，它们在静态引入这个functions的包，然后又看了一下两个class
						//才发现的最最终原因...还真是坑
						//不过确实是在java1.5中引入的静态导入包，具体回忆去看看JavaBase包下的记录吧
						//找到原因就可以愉快的写代码了，哈哈哈。201810151944
						avg(employee.col("salary")),
						max(employee.col("salary")),
						//把基本的functions的常用方法都加上跑一圈，具体的所有方法已经吧functions.class
						//收藏了，需要用的时候可以去里面在看看.201810151950
						min(employee.col("salary")),
						sum(employee.col("salary")),
						count(department.col("name")),
						functions.countDistinct(department.col("name"))
				)
				/*
				// The following are equivalent(相等的):
			   peopleDs.filter($"age" > 15)
			   peopleDs.where($"age" > 15)
				 *filter()和where()算法是一样的
				 */
				.where("name !='Leo'")
				.show();
/*

+--------------------+------+-----------+-----------+-----------+-----------+-----------+--------------------+
|                name|gender|avg(salary)|max(salary)|min(salary)|sum(salary)|count(name)|count(DISTINCT name)|
+--------------------+------+-----------+-----------+-----------+-----------+-----------+--------------------+
|Technical Department|  male|    17500.0|      20000|      15000|      35000|          2|                   1|
|       HR Department|female|    21000.0|      21000|      21000|      21000|          1|                   1|
|Financial Department|female|    26500.0|      28000|      25000|      53000|          2|                   1|
|       HR Department|  male|    18000.0|      18000|      18000|      18000|          1|                   1|
+--------------------+------+-----------+-----------+-----------+-----------+-----------+--------------------+
*/

		employee.createOrReplaceTempView("employee_view");
		department.createOrReplaceTempView("department_view");
		//department.select("gender").show();

		Dataset<Row> avgDF = spark.sql(
				"select department_view.name,employee_view.gender," +
						"avg(employee_view.salary),max(employee_view.age) from " +
						"employee_view,department_view " +
						"where employee_view.depId = department_view.id " +
						"group by department_view.name,employee_view.gender"
		);
		/*虽然直接用sql页是没问题可以实现的，但是总要创建两个临时表，代码量太多不简洁。201801151848
				+--------------------+------+------------------+--------+
				|                name|gender|       avg(salary)|max(age)|
				+--------------------+------+------------------+--------+
				|       HR Department|female|           21000.0|      21|
				|Technical Department|  male|           17500.0|      35|
				|Financial Department|female|20333.333333333332|      30|
				|       HR Department|  male|           18000.0|      42|
				+--------------------+------+------------------+--------+*/

		//avgDF.show();
		//哇哦原来这里可以直接拿到sql的执行计划 201801152001
		avgDF.explain();
		spark.sql(
				"select department_view.name,employee_view.gender," +
						"avg(employee_view.salary),max(employee_view.age) from " +
						"employee_view,department_view " +
						"where employee_view.depId = department_view.id " +
						"group by department_view.name,employee_view.gender"
		).explain();
/*

	== Physical Plan ==
	*HashAggregate(keys=[name#28, gender#10], functions=[avg(salary#12L), max(age#8L)])
			+- Exchange hashpartitioning(name#28, gender#10, 200)
					+- *HashAggregate(keys=[name#28, gender#10], functions=[partial_avg(salary#12L), partial_max(age#8L)])
			+- *Project [age#8L, gender#10, salary#12L, name#28]
			+- *BroadcastHashJoin [depId#9L], [id#27L], Inner, BuildRight
				:- *Project [age#8L, depId#9L, gender#10, salary#12L]
				:  +- *Filter isnotnull(depId#9L)
				:     +- *FileScan json [age#8L,depId#9L,gender#10,salary#12L] Batched: false, Format: JSON, Location: InMemoryFileIndex[file:/Users/yangyu/IdeaProjects/SparkPractise/src/resource/employee.json], PartitionFilters: [], PushedFilters: [IsNotNull(depId)], ReadSchema: struct<age:bigint,depId:bigint,gender:string,salary:bigint>
					+- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]))
			+- *Project [id#27L, name#28]
			+- *Filter isnotnull(id#27L)
			+- *FileScan json [id#27L,name#28] Batched: false, Format: JSON, Location: InMemoryFileIndex[file:/Users/yangyu/IdeaProjects/SparkPractise/src/resource/department.json], PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint,name:string>
*/

	}

}
