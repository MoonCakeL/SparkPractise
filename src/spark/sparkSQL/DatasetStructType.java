package spark.sparkSQL;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import spark.sparkSQL.util.ConvertType;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

public class DatasetStructType {
	public static void main(String[] args)  throws Exception{
		SparkSession spark = SparkSession.builder()
				.master("local").appName("DatasetStructType").getOrCreate();

		JavaRDD<String> dataSourceRDD = spark.read()
				.textFile("/test/2014bus.txt").javaRDD();

		//System.out.println(dataSourceRDD.take(3));

		/*JavaRDD<String> listJavaRDD = dataSourceRDD.flatMap(
				new FlatMapFunction<String, String>() {
					private static final long serialVersionUID = -6963326363176410516L;

					@Override
					public Iterator<String> call(String s) {
						return Arrays.asList(s.split("/n")).iterator();
					}
				}
		);*/

		//System.out.println(listJavaRDD.take(1));

		//根据Row数组对象下标位置，定义元数据转换需要的数据类型
		LinkedHashMap<Integer,DataType> dataMap = new LinkedHashMap<>();
		dataMap.put(0,DataTypes.StringType);
		dataMap.put(1,DataTypes.IntegerType);
		dataMap.put(2,DataTypes.StringType);
		dataMap.put(3,DataTypes.StringType);
		dataMap.put(4,DataTypes.IntegerType);
		dataMap.put(5,DataTypes.IntegerType);
		dataMap.put(6,DataTypes.IntegerType);

		JavaRDD<Row> rowJavaRDD = dataSourceRDD.map(
				new Function<String, Row>() {
					private static final long serialVersionUID = -3133268667673184179L;

					@Override
					public Row call(String s) throws Exception {
						String[] arrayString = s.split(",");
						//Object[] objects = null;
						LinkedList<Object> dataLink = new LinkedList<>();

						for(Map.Entry<Integer,DataType> entry : dataMap.entrySet()){

							dataLink.add(ConvertType.convertDataType
									(arrayString[entry.getKey()],entry.getValue()));
						}

					//System.out.println(arrayString.length);
						return RowFactory.create(dataLink.toArray());
					}
				}
		);

		System.out.println(rowJavaRDD.take(1));


		/*LinkedList<String> schemaString = Arrays.asList(
				(T)"cardID","lineID","beginTime","endTime","stationFrom","stationTo","busID"
		);*/

		LinkedList<StructField> fields = new LinkedList<>();

		LinkedHashMap<String,String> schemaMap = new LinkedHashMap();

		//定义字段对应创建schema的spark类型（spark本身不会进行对元数据的类型转换，需要对Row对象中数据先进行转换）
		schemaMap.put("cardID","string");
		schemaMap.put("lineID","int");
		schemaMap.put("beginTime","string");
		schemaMap.put("endTime","string");
		schemaMap.put("stationFrom","int");
		schemaMap.put("stationTo","int");
		schemaMap.put("busID","int");

		for(Map.Entry<String,String> entry : schemaMap.entrySet()){

			//此处特别需要说明一下在调用DataTypes.createStructField()方法创建StructField时
			//需要指定StructField的数据类型，此处非string的数据必须先进行转换，然后在给StructField
			//添加对应的类型，spark有自己的数据类型，但是他不会去自动转换，
			//所以需要先转换好对应的数据类型，再给这个数据对应的StructField字段添加响应的数据类型
			//不然会保存，因为一般原始进来的rdd都是<String>类型的，需要先转换你所需的其它类型，再给
			//StructField设置对应的数据类型
			StructField field = DataTypes.createStructField(
					entry.getKey(),
					ConvertType.ConvertTypetoHive(entry.getValue()),
					true);

			fields.add(field);
		}

		StructType schema = DataTypes.createStructType(fields);

		Dataset<Row> dataDF = spark.createDataFrame(rowJavaRDD,schema);

		dataDF.show();
	}
}
