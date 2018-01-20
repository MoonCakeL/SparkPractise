package HttpNetLog;

import java.io.Serializable;

/**
 * spark基本代码的编程学习基本结束了，正好前几天在论坛获得几份
 * 样例数据，可以好好写写代码了，先来一个httpnet日志的数据
 * 文件大小30mb，29个字段，在已知结构情况下在做一个case class出来
 * 201801162030
 */

public  class HttpNetCaseClass implements Serializable {
	private Long 	BeginTime;	//采集第一个数据包的时间
	private Long 	EndTime;	//采集最后一个数据包的时间
	private String 	MSISDN;	//用户的手机号码
	private String 	SourceIP;	//用户的IP地址
	private String 	SourcePort;	//用户的端口号
	private String 	APIP;	//APIP
	private String 	APMAC;	//APMAC
	private String 	ACIP;	//ACIP
	private String 	ACMAC;	//ACMAC
	private String 	RequestType;	//GETPOST
	private String 	DestinationIP; //用户访问的业务平台的IP地址
	private String 	DestinationPort;	//访问的目标端口号
	private String 	Service;	//详细的业务类型,例如，QQ，迅雷等
	private String	ServiceType1;	//业务组类型。相似的一组业务为一组，例如浏览下载，P2P下载、即时通信等
	private String	ServiceType2;	//业务组类型2
	private String	URL;	//访问URL
	private String	Domain;	//访问域名(HOST)
	private String	SiteName;	//网址名称
	private String	SiteType1;	//网址分类
	private String	SiteType2;	//网址2级分类
	private String	ICP;	//服务提供商
	private String	UpPackNum;	//上行数据包数，单位：个
	private String	DownPackNum;	//下行数据包数，单位：个
	private String	UpPayLoad;	//上行总流量。要注意单位的转换，单位：byte
	private String	DownPayLoad;	//下行总流量。要注意单位的转换，单位：byte
	private String	HttpStatus;	//HTTP Response的状态
	private String	UA;	//User Agent 手机类型，如苹果、安卓
	private String	ClientType;	//1：浏览器；2：客户端。
	private Long	ResponseTime;	//第一个包回应时间

	public HttpNetCaseClass() {
	}

	public Long getBeginTime() {
		return BeginTime;
	}

	public void setBeginTime(Long beginTime) {
		BeginTime = beginTime;
	}

	public Long getEndTime() {
		return EndTime;
	}

	public void setEndTime(Long endTime) {
		EndTime = endTime;
	}

	public String getMSISDN() {
		return MSISDN;
	}

	public void setMSISDN(String MSISDN) {
		this.MSISDN = MSISDN;
	}

	public String getSourceIP() {
		return SourceIP;
	}

	public void setSourceIP(String sourceIP) {
		SourceIP = sourceIP;
	}

	public String getSourcePort() {
		return SourcePort;
	}

	public void setSourcePort(String sourcePort) {
		SourcePort = sourcePort;
	}

	public String getAPIP() {
		return APIP;
	}

	public void setAPIP(String APIP) {
		this.APIP = APIP;
	}

	public String getAPMAC() {
		return APMAC;
	}

	public String getACIP() {
		return ACIP;
	}

	public void setACIP(String ACIP) {
		this.ACIP = ACIP;
	}
	public void setAPMAC(String APMAC) {
		this.APMAC = APMAC;
	}

	public String getACMAC() {
		return ACMAC;
	}

	public void setACMAC(String ACMAC) {
		this.ACMAC = ACMAC;
	}

	public String getRequestType() {
		return RequestType;
	}

	public void setRequestType(String requestType) {
		RequestType = requestType;
	}

	public String getDestinationIP() {
		return DestinationIP;
	}

	public void setDestinationIP(String destinationIP) {
		DestinationIP = destinationIP;
	}

	public String getDestinationPort() {
		return DestinationPort;
	}

	public void setDestinationPort(String destinationPort) {
		DestinationPort = destinationPort;
	}

	public String getService() {
		return Service;
	}

	public void setService(String service) {
		Service = service;
	}

	public String getServiceType1() {
		return ServiceType1;
	}

	public void setServiceType1(String serviceType1) {
		ServiceType1 = serviceType1;
	}

	public String getServiceType2() {
		return ServiceType2;
	}

	public void setServiceType2(String serviceType2) {
		ServiceType2 = serviceType2;
	}

	public String getURL() {
		return URL;
	}

	public void setURL(String URL) {
		this.URL = URL;
	}

	public String getDomain() {
		return Domain;
	}

	public void setDomain(String domain) {
		Domain = domain;
	}

	public String getSiteName() {
		return SiteName;
	}

	public void setSiteName(String siteName) {
		SiteName = siteName;
	}

	public String getSiteType1() {
		return SiteType1;
	}

	public void setSiteType1(String siteType1) {
		SiteType1 = siteType1;
	}

	public String getSiteType2() {
		return SiteType2;
	}

	public void setSiteType2(String siteType2) {
		SiteType2 = siteType2;
	}

	public String getICP() {
		return ICP;
	}

	public void setICP(String ICP) {
		this.ICP = ICP;
	}

	public String getUpPackNum() {
		return UpPackNum;
	}

	public void setUpPackNum(String upPackNum) {
		UpPackNum = upPackNum;
	}

	public String getDownPackNum() {
		return DownPackNum;
	}

	public void setDownPackNum(String downPackNum) {
		DownPackNum = downPackNum;
	}

	public String getUpPayLoad() {
		return UpPayLoad;
	}

	public void setUpPayLoad(String upPayLoad) {
		UpPayLoad = upPayLoad;
	}

	public String getDownPayLoad() {
		return DownPayLoad;
	}

	public void setDownPayLoad(String downPayLoad) {
		DownPayLoad = downPayLoad;
	}

	public String getHttpStatus() {
		return HttpStatus;
	}

	public void setHttpStatus(String httpStatus) {
		HttpStatus = httpStatus;
	}

	public String getUA() {
		return UA;
	}

	public void setUA(String UA) {
		this.UA = UA;
	}

	public String getClientType() {
		return ClientType;
	}

	public void setClientType(String clientType) {
		ClientType = clientType;
	}

	public Long getResponseTime() {
		return ResponseTime;
	}

	public void setResponseTime(Long responseTime) {
		ResponseTime = responseTime;
	}
}
