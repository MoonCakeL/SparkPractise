package spark.sparkSQL;

public class test {
	public static void main(String[] args){
		/*SparkSession spark = SparkSession.builder()
				.master("local").getOrCreate();

		JavaSparkContext js = new JavaSparkContext(spark.sparkContext());

		JavaRDD<String> javaRDD = js.parallelize(Arrays.asList("1,2,3"));

		javaRDD<Row> rowjavaRDD = javaRDD.flatMap(
				new FlatMapFunction<String, Row>() {
					@Override
					public Iterator<Row> call(String s) throws Exception {
						//String[] line = s.split(",");
						return Arrays.asList(s.split(","));
					}
				}
		)

		String schemaString = "ID1,ID2,ID3";

		List<StructField> structTypeList = new ArrayList<>();
		for(String fielName : schemaString.split(",")){
			StructField field = DataTypes.createStructField(fielName,
					DataTypes.IntegerType,true);

			structTypeList.add(field);
		}

		StructType schema = DataTypes.createStructType(structTypeList);

		Dataset<Row> df = spark.createDataFrame(javaRDD,schema);*/
	}
}
