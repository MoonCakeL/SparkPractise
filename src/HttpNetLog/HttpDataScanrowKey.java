package HttpNetLog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import sparkHbase.example.utils.HbaseConnectFactory;

public class HttpDataScanrowKey {
	private static final Logger logger = LoggerFactory.getLogger(HttpDataScanrowKey.class);

	/*public static void main(String[] args){
		JavaRDD<byte[]> tmpRDD = getrowKey();

		tmpRDD.foreach(x -> System.out.println("单个rowkey："+ Bytes.toString(x)));
		//bed5e858-10.80.58.44

	}*/

	public JavaRDD<byte[]> getrowKey (){

		SparkConf sparkConf = new SparkConf()
				//.setMaster("local")
				.setAppName("scanRowKey & GetRoyKeytoHive");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		JavaRDD<byte[]> rowKeyRDD = null;
		try {

			TableName tableName = TableName.valueOf("hbaseHttpByload");
			Configuration conf = HbaseConnectFactory.getInstance().getHBaseConfiguration();

			JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc,conf);
			try {
				if (HbaseConnectFactory.getInstance()
						.getHbaseConnect().getAdmin().tableExists(tableName)){

					/*设置将传递给扫描器的缓存行数。如果未设置，则将应用配置设置
					HConstants.HBASE_CLIENT_SCANNER_CACHING。
					更高的缓存值将使扫描仪更快，但会使用更多的内存。 */
					Scan scan = new Scan();
					scan.setCaching(100);

					//ImmutableBytesWritable用于对hbase查询时，对底层HFile中数据进行
					//序列化而使用
					JavaRDD<Tuple2<ImmutableBytesWritable, Result>> hbaseRDD =
							hbaseContext.hbaseRDD(tableName, scan);

					//logger.info("从HBase中Scan获得的条数："+hbaseRDD.count());
						rowKeyRDD = hbaseRDD.map(
									x -> x._2.getRow()
							);
					}

			}catch (Exception e){
				logger.error("表："+tableName.toString()+"不存在...");
			}
		}catch (Exception e){
			logger.error("实例化HbaseConntext失败..."+e.getMessage());
		}
		logger.info("本次查询到rowKey个数为："+rowKeyRDD.count());
		return rowKeyRDD;

	}
}
