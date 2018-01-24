package sparkElasticsearch.examples;


import org.elasticsearch.hadoop.rest.RestClient;
import sparkElasticsearch.examples.util.ElasticsearchConnFactory;

public class test {
	public static void main(String[] args){

		RestClient escClient = ElasticsearchConnFactory.getInstance().getESConnFactory();

		System.out.println(escClient.getCurrentNode());

	}
}
