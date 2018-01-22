package HttpNetLog.utils;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.util.HashMap;
import java.util.Map;

public class GetHttpNetHBaseType {

	public static final Map<String,DataType> hbaseGetDataType = new HashMap<String,DataType>(){

		{
			put("BeginTime", DataTypes.LongType);  //BeginTime;	//采集第一个数据包的时间
			put("EndTime",DataTypes.LongType);  //EndTime;	//采集最后一个数据包的时间
			put("MSISDN",DataTypes.StringType);  //MSISDN;	//用户的手机号码
			put("SourceIP",DataTypes.StringType);  //SourceIP;	//用户的IP地址
			put("SourcePort",DataTypes.StringType);  //SourcePort;	//用户的端口号
			put("APIP",DataTypes.StringType);  //APIP;	//APIP
			put("APMAC",DataTypes.StringType);  //APMAC;	//APMAC
			put("ACIP",DataTypes.StringType);  //ACIP;	//ACIP
			put("ACMAC",DataTypes.StringType);  //ACMAC;	//ACMAC
			put("RequestType",DataTypes.StringType);  //RequestType;	//GETPOST
			put("DestinationIP",DataTypes.StringType);  //DestinationIP; //用户访问的业务平台的IP地址
			put("DestinationPort",DataTypes.StringType);  //DestinationPort;	//访问的目标端口号
			put("Service",DataTypes.StringType);  //Service;	//详细的业务类型,例如，QQ，迅雷等
			put("ServiceType1",DataTypes.StringType);  //	ServiceType1;	//业务组类型。相似的一组业务为一组，例如浏览下载，P2P下载、即时通信等
			put("ServiceType2",DataTypes.StringType);  //	ServiceType2;	//业务组类型2
			put("URL",DataTypes.StringType);  //	URL;	//访问URL
			put("Domain",DataTypes.StringType);  //	Domain;	//访问域名(HOST);
			put("SiteName",DataTypes.StringType);  //	SiteName;	//网址名称
			put("SiteType1",DataTypes.StringType);  //	SiteType1;	//网址分类
			put("SiteType2",DataTypes.StringType);  //	SiteType2;	//网址2级分类
			put("ICP",DataTypes.StringType);  //	ICP;	//服务提供商
			put("UpPackNum",DataTypes.StringType);  //	UpPackNum;	//上行数据包数，单位：个
			put("DownPackNum",DataTypes.StringType);  //	DownPackNum;	//下行数据包数，单位：个
			put("UpPayLoad",DataTypes.StringType);  //	UpPayLoad;	//上行总流量。要注意单位的转换，单位：byte
			put("DownPayLoad",DataTypes.StringType);  //	DownPayLoad;	//下行总流量。要注意单位的转换，单位：byte
			put("HttpStatus",DataTypes.StringType);  //	HttpStatus;	//HTTP Response的状态
			put("UA",DataTypes.StringType);  //	UA;	//User Agent 手机类型，如苹果、安卓
			put("ClientType",DataTypes.StringType);  //	ClientType;	//1：浏览器；2：客户端。
			put("ResponseTime",DataTypes.LongType);  //		ResponseTime;	//第一个包回应时间
		}

	};
}
