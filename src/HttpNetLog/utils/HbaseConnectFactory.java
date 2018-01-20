package HttpNetLog.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 连接HBase集群
 * @author yangyu
 *
 */

public class HbaseConnectFactory {
	
	private static final Logger logger = LoggerFactory.getLogger(HttpNetLog.utils.HbaseConnectFactory.class);
	

	private static String CLUSTER_NAME = "192.168.42.24,192.168.42.25,192.168.42.26";
	private static Connection hbaseConnect;
	
	//3.构造方法实现部分
	private  HbaseConnectFactory() {
		if (CLUSTER_NAME == null){
			logger.error("获取连接信息失败！！！");
		} else {
			try {
				Configuration config = HBaseConfiguration.create();
				config.set("hbase.zookeeper.quorum",CLUSTER_NAME);
				
				hbaseConnect = ConnectionFactory.createConnection(config);
				logger.info("Hbase 连接成功！");
            } catch (IOException e) {
            	logger.error("Hbase 连接失败！" + e.getMessage());
            	}		
			}	
	}
	
	//2.饿汉模式：类加载的时候就创建对象，创建实例，调用构造函数
	private static HttpNetLog.utils.HbaseConnectFactory HbaseConnectFactory = new HbaseConnectFactory();
	
	//1.static静态类型项目启动就直接在内存中存在了，(直接完成“饿汉模式”)类加载的时候就创建对象
	//外部调用getInstance方法获取实例，再.getHbaseConnect()获取连接
	public static HttpNetLog.utils.HbaseConnectFactory getInstance(){
		return HbaseConnectFactory;
	}
	//4.创建实例后，获取构造函数实例中的连接
	public static Connection getHbaseConnect(){
		return hbaseConnect;		
	}
}
