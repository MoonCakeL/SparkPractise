package HttpNetLog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import sparkHbase.example.utils.HbaseConnectFactory;

public class HttpDataScanrowKey {
	private static final Logger logger = LoggerFactory.getLogger(HttpDataScanrowKey.class);

	public static void main(String[] args){
		getrowKey();

	}

	public static void  /*JavaRDD<byte[]>*/ getrowKey (){

		SparkConf sparkConf = new SparkConf()
				.setMaster("local")
				.setAppName("scanRowKey");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		JavaRDD<byte[]> rowKeyRDD = null;
		try {

			TableName tableName = TableName.valueOf("hbasetest");
			Configuration conf = HbaseConnectFactory.getInstance().getHBaseConfiguration();

			JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc,conf);
			try {
				if (HbaseConnectFactory.getInstance()
						.getHbaseConnect().getAdmin().tableExists(tableName)){
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

		//return rowKeyRDD;
		rowKeyRDD.foreach(
				x -> System.out.println(Bytes.toString(x))
		);
	}
}
