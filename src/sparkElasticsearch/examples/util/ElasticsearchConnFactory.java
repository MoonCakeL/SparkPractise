package sparkElasticsearch.examples.util;

import org.elasticsearch.hadoop.cfg.PropertiesSettings;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ElasticsearchConnFactory {

	private static final Logger logger = LoggerFactory.getLogger(ElasticsearchConnFactory.class);

	private static RestClient client;

	private ElasticsearchConnFactory(){
		if (client == null){
		//连接es参数
			Map<String,String> esMap = new HashMap<>();
			esMap.put("es.nodes","192.168.42.29,192.168.42.30,192.168.42.31");
			esMap.put("es.port","9200");

			Settings setting = new PropertiesSettings().merge(esMap);
			try {
				client = new RestClient(setting);
			}catch (Exception e){
				logger.error(e.getMessage());
			}
		}

	}

	//懒汉式加载
	private static ElasticsearchConnFactory ElasticsearchConnFactory = new ElasticsearchConnFactory();
	//1.先获取实例，创建类实例，返回创建完毕的类实例
	public static ElasticsearchConnFactory getInstance(){
		return ElasticsearchConnFactory;
	}
	//2.类实例化之后再get连接
	public RestClient getESConnFactory(){
		return client;
	}

	//3.关闭连接

	public static void cloesESClient(){
	  client.close();
	  client=null;
	}


}
