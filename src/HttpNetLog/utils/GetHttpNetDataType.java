package HttpNetLog.utils;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class GetHttpNetDataType implements Serializable {

	public final static String ColumnFamily = "D";

	public final static Map httpGetDataType = new HashMap()
	{
		{
			put(0,"Long");  //BeginTime;	//采集第一个数据包的时间
			put(1,"Long");  //EndTime;	//采集最后一个数据包的时间
			put(2,"String");  //MSISDN;	//用户的手机号码
			put(3,"String");  //SourceIP;	//用户的IP地址
			put(4,"String");  //SourcePort;	//用户的端口号
			put(5,"String");  //APIP;	//APIP
			put(6,"String");  //APMAC;	//APMAC
			put(7,"String");  //ACIP;	//ACIP
			put(8,"String");  //ACMAC;	//ACMAC
			put(9,"String");  //RequestType;	//GETPOST
			put(10,"String");  //DestinationIP; //用户访问的业务平台的IP地址
			put(11,"String");  //DestinationPort;	//访问的目标端口号
			put(12,"String");  //Service;	//详细的业务类型,例如，QQ，迅雷等
			put(13,"String");  //	ServiceType1;	//业务组类型。相似的一组业务为一组，例如浏览下载，P2P下载、即时通信等
			put(14,"String");  //	ServiceType2;	//业务组类型2
			put(15,"String");  //	URL;	//访问URL
			put(16,"String");  //	Domain;	//访问域名(HOST);
			put(17,"String");  //	SiteName;	//网址名称
			put(18,"String");  //	SiteType1;	//网址分类
			put(19,"String");  //	SiteType2;	//网址2级分类
			put(20,"String");  //	ICP;	//服务提供商
			put(21,"String");  //	UpPackNum;	//上行数据包数，单位：个
			put(22,"String");  //	DownPackNum;	//下行数据包数，单位：个
			put(23,"String");  //	UpPayLoad;	//上行总流量。要注意单位的转换，单位：byte
			put(24,"String");  //	DownPayLoad;	//下行总流量。要注意单位的转换，单位：byte
			put(25,"String");  //	HttpStatus;	//HTTP Response的状态
			put(26,"String");  //	UA;	//User Agent 手机类型，如苹果、安卓
			put(27,"String");  //	ClientType;	//1：浏览器；2：客户端。
			put(28,"Long");  //		ResponseTime;	//第一个包回应时间
		}


	};

	public final static Map httpNetLogColumn = new HashMap<Integer,String>(){
		{
			put(0,"BeginTime");  //BeginTime;	//采集第一个数据包的时间
			put(1,"EndTime");  //EndTime;	//采集最后一个数据包的时间
			put(2,"MSISDN");  //MSISDN;	//用户的手机号码
			put(3,"SourceIP");  //SourceIP;	//用户的IP地址
			put(4,"SourcePort");  //SourcePort;	//用户的端口号
			put(5,"APIP");  //APIP;	//APIP
			put(6,"APMAC");  //APMAC;	//APMAC
			put(7,"ACIP");  //ACIP;	//ACIP
			put(8,"ACMAC");  //ACMAC;	//ACMAC
			put(9,"RequestType");  //RequestType;	//GETPOST
			put(10,"DestinationIP");  //DestinationIP; //用户访问的业务平台的IP地址
			put(11,"DestinationPort");  //DestinationPort;	//访问的目标端口号
			put(12,"Service");  //Service;	//详细的业务类型,例如，QQ，迅雷等
			put(13,"ServiceType1");  //	ServiceType1;	//业务组类型。相似的一组业务为一组，例如浏览下载，P2P下载、即时通信等
			put(14,"ServiceType2");  //	ServiceType2;	//业务组类型2
			put(15,"URL");  //	URL;	//访问URL
			put(16,"Domain");  //	Domain;	//访问域名(HOST);
			put(17,"SiteName");  //	SiteName;	//网址名称
			put(18,"SiteType1");  //	SiteType1;	//网址分类
			put(19,"SiteType2");  //	SiteType2;	//网址2级分类
			put(20,"ICP");  //	ICP;	//服务提供商
			put(21,"UpPackNum");  //	UpPackNum;	//上行数据包数，单位：个
			put(22,"DownPackNum");  //	DownPackNum;	//下行数据包数，单位：个
			put(23,"UpPayLoad");  //	UpPayLoad;	//上行总流量。要注意单位的转换，单位：byte
			put(24,"DownPayLoad");  //	DownPayLoad;	//下行总流量。要注意单位的转换，单位：byte
			put(25,"HttpStatus");  //	HttpStatus;	//HTTP Response的状态
			put(26,"UA");  //	UA;	//User Agent 手机类型，如苹果、安卓
			put(27,"ClientType");  //	ClientType;	//1：浏览器；2：客户端。
			put(28,"ResponseTime");  //		ResponseTime;	//第一个包回应时间
		}
	};

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
		//统一转换为小写
		switch (type.toLowerCase()){
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
