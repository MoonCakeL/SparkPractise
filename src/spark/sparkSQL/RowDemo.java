package spark.sparkSQL;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RowDemo {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
										 .appName("Row")
										 .master("local")
										 .getOrCreate();
		
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		
		ArrayList<String> listRow = new ArrayList<String>(Arrays.asList("name","createtime","type","id"));
		List<List<String>> k = new ArrayList<List<String>>();
		k.add(listRow);
		 		
		String[] arrayRow = {"name","createtime","type","id"};
		String[][] arrayRow2 = {
									{"name"},{"createtime"},
									{"type"},{"id"}
								};
		
		JavaRDD<List<String>> javaRDDlistRow = sc.parallelize(k);

		//创建Row对象
		Row row1 = RowFactory.create(k);
		Row row2 = RowFactory.create((Object[]) arrayRow);
		Row row3 = RowFactory.create((Object[]) arrayRow2);
		
		//row.getList(0);
		//Row 对象表示DataFrame(SchemaRDD1.3中叫法) 中的记录（数据），
		// 其本质就是一个定长的字段数组(一维)。
		/*java.util.ArrayList cannot be cast to java.lang.String
		System.out.println("row1.toString()" + row1.length() + row1.mkString() + row1.getString(0));
*/
		System.out.println("row2 toString: " + row2.toString());
		System.out.println("row2 length:" + row2.length());
		System.out.println("row2 getString（0）: " + row2.getString(0));
		System.out.println("row2 getString（3）: " + row2.getString(3));
		System.out.println("row2 mkString:" + row2.mkString());
		
		//System.out.println("row3.toString()：" + row3.length());
		//JavaRDD<Row> rowRDD = javaRdd.map(y -> RowFactory.create(y.toArray()));
		//JavaRDD<Row> javaRDDRow = javaRDDlistRow.map(f -> RowFactory.create(y.toArray()));
		
		//System.out.println(row2.toSeq().toString());

	}

}
