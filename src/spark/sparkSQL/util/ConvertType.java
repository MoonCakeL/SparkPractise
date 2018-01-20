package spark.sparkSQL.util;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Timestamp;

public class ConvertType {

	/**
	 *传递给StructField对应的数据字段进行转换，spark不会字段完成类型转换
	 * 需要将实际数据转换完毕后，进行StructField对应的数据类型添加
	 * @param str
	 * @param dataType
	 * @return
	 * @throws Exception
	 */
	public static Object convertDataType(String str, DataType dataType) throws Exception {

		Object result = null;

		switch (dataType.toString()) {
			case "IntegerType":
				result = Integer.valueOf(str);
				break;
			case "LongType":
				result = Long.valueOf(str);
				break;
			case "FloatType":
				result = Float.valueOf(str);
				break;
			case "DoubleType":
				result = Double.valueOf(str);
				break;
			case "BooleanType":
				result = Boolean.parseBoolean(str);
				break;
			case "StringType":
				result = str;
				break;
			case "TimestampType":
				result = new Timestamp(Long.valueOf(str));
				break;
			default:
				result = str;
				break;
		}
		return result;
	}
	/*
	toUpperCase的意思是将所有的英文字符转换为大写字母，如：
	String  cc = “aBc123”.toUpperCase();结果就是：ABC123。

	toLowerCase的意思是将所有的英文字符转换为小写字母，如：
	String  cc = “aBc”.toUpperCase();结果就是：abc123。

	备注：这两个方法只对英文字母有效，对除了A~Z和a~z的其余字符无任何效果。
	*/

	/**
	 * 将传入的类型转换为Hive的数据类型
	 * @param type
	 * @return
	 */
	public static DataType ConvertTypetoHive(String type){
		DataType hiveDataType = DataTypes.StringType;
		type.toUpperCase();
		switch (type){
			case "int":
				hiveDataType = DataTypes.IntegerType;
				break;
			case "long":
				hiveDataType = DataTypes.LongType;
				break;
			case "string":
				hiveDataType = DataTypes.StringType;
				break;
			case "float":
				hiveDataType = DataTypes.FloatType;
				break;
			case "double":
				hiveDataType = DataTypes.DoubleType;
				break;
			case "timeStamp":
				hiveDataType = DataTypes.TimestampType;
				break;
			case "boolean":
				hiveDataType = DataTypes.BooleanType;
				break;
		}
		return hiveDataType;
	}
}
