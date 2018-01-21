package sparkHbase.example.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import sparkHbase.example.utils.HbaseConnectFactory;

import java.util.Iterator;
import java.util.List;

public class ResultAndCell {

	private static final Logger logger = LoggerFactory.getLogger(ResultAndCell.class);

	public static void main(String[] args){
		ResultAndCell scanRowKey = new ResultAndCell();
		scanRowKey.scanRowKey();

	}

	public JavaRDD<String> scanRowKey(){
		SparkConf sparkConf = new SparkConf()
				.setMaster("local")
				.setAppName("scanRowKey");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		try {

			TableName tableName = TableName.valueOf("hbasetest");
			Configuration conf = HbaseConnectFactory.getInstance().getHBaseConfiguration();

			JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc,conf);
			try {
					if (HbaseConnectFactory.getInstance().getHbaseConnect().getAdmin().tableExists(tableName)){
						Scan scan = new Scan();
						scan.setCaching(100);

						//ImmutableBytesWritable用于对hbase查询时，对底层HFile中数据进行
						//序列化而使用
						JavaRDD<Tuple2<ImmutableBytesWritable, Result>> hbaseRDD =
								hbaseContext.hbaseRDD(tableName, scan);

						hbaseRDD.foreach(
// HBase table split(table name: hbasetest, scan: {"loadColumnFamiliesOnDemand":null,"startRow":"","stopRow":"","batch":-1,"cacheBlocks":true,"totalColumns":0,"maxResultSize":-1,"families":{},"caching":100,"maxVersions":1,"timeRange":[0,9223372036854775807]}, start row: , end row: , region location: localhost, encoded region name: 7a4d109f4862964a294ddd1e3c92cf48)
								x -> {
									//ResultFunction resultFunction = new ResultFunction();
									//System.out.println("数据内容："+resultFunction.call(x._2));

									//String data =resultFunction.call(x._2);

									/*
									 *　以下是两种遍历Result数组的方法
									 * 第一种是通过Result的getRow()和getValue()两个方法实现
									 * 第二种使用Result类的rawCells()方法返回一个Cell数组
									 */

									System.out.println("打印："+
													// 取到rowkey，并转化为字符串
													Bytes.toString(x._2.getRow())
									+"value"+
											//如果result实例中中包含指定列，就取出其列值，并转化为字符串
													Bytes.toString(x._2.getValue(Bytes.toBytes("D"),Bytes.toBytes("Service")))
											//打印：bed5e858-10.80.58.44value掌中新浪
									);


									List<Cell> listCell = x._2.listCells();


										for (Cell cell : listCell){
											// result.rawCells()会返回一个Cell[]，这个Cell[]存放的对应列的多个版本值，默认只取最新的版本，遍历其中的每个Cell对象
											/*Interface Cell
											public interface Cell

											The unit of storage in HBase consisting of the following fields:
											1) row
											2) column family
											3) column qualifier
											4) timestamp
											5) type
											6) MVCC version
											7) value*/
											// 在更早的版本中是使用KeyValue类来实现，但是KeyValue在0.98中已经废弃了，改用Cell
											// cell.getRowArray()    得到数据的byte数组
											// cell.getRowOffset()    得到rowkey在数组中的索引下标
											// cell.getRowLength()    得到rowkey的长度
											// 将rowkey从数组中截取出来并转化为String类型

											//获取值数组
											byte[] valueArray = cell.getValueArray();
											//长度
											int valuelength = cell.getValueLength();
											int valueoffset = cell.getValueOffset();
											byte[] tempvaluearray = new byte[valuelength];
											System.arraycopy(valueArray, valueoffset, tempvaluearray, 0, valuelength);
											String tempvalue = Bytes.toString(tempvaluearray);

											System.out.println("value:" + tempvalue);
										/*	value:
======
											value:
======
											value:
======
											value:
======
											value:1368607184116
													======
											value:2
													======
											value:111.13.87.17
													======
											value:80
													======
											value:
======
											value:0
													======
											value:0
													======
											value:1368607184116
													======
											value:200
													======
											value:111.13.87.17
													======
											value:10.80.58.44
													======
											value:
======
											value:0
													======
											value:掌中新浪
													======
											value:生活软件
													======
											value:门户
													======
											value:111.13.87.17
													======
											value:
======
											value:
======
											value:10.80.58.44
													======
											value:35679
													======
											value:
======
											value:
======
											value:1
													======
											value:66

											bed5e858-10.80.58.44/D:ACIP/1516532769734/Put/vlen=0/seqid=0
											bed5e858-10.80.58.44/D:ACMAC/1516532769734/Put/vlen=0/seqid=0
											bed5e858-10.80.58.44/D:APIP/1516532769734/Put/vlen=0/seqid=0
											bed5e858-10.80.58.44/D:APMAC/1516532769734/Put/vlen=0/seqid=0
											bed5e858-10.80.58.44/D:BeginTime/1516532769734/Put/vlen=13/seqid=0
											bed5e858-10.80.58.44/D:ClientType/1516532769734/Put/vlen=1/seqid=0
											bed5e858-10.80.58.44/D:DestinationIP/1516532769734/Put/vlen=12/seqid=0
											bed5e858-10.80.58.44/D:DestinationPort/1516532769734/Put/vlen=2/seqid=0
											bed5e858-10.80.58.44/D:Domain/1516532769734/Put/vlen=0/seqid=0
											bed5e858-10.80.58.44/D:DownPackNum/1516532769734/Put/vlen=1/seqid=0
											bed5e858-10.80.58.44/D:DownPayLoad/1516532769734/Put/vlen=1/seqid=0
											bed5e858-10.80.58.44/D:EndTime/1516532769734/Put/vlen=13/seqid=0
											bed5e858-10.80.58.44/D:HttpStatus/1516532769734/Put/vlen=3/seqid=0
											bed5e858-10.80.58.44/D:ICP/1516532769734/Put/vlen=12/seqid=0
											bed5e858-10.80.58.44/D:MSISDN/1516532769734/Put/vlen=11/seqid=0
											bed5e858-10.80.58.44/D:RequestType/1516532769734/Put/vlen=0/seqid=0
											bed5e858-10.80.58.44/D:ResponseTime/1516532769734/Put/vlen=1/seqid=0
											bed5e858-10.80.58.44/D:Service/1516532769734/Put/vlen=12/seqid=0
											bed5e858-10.80.58.44/D:ServiceType1/1516532769734/Put/vlen=12/seqid=0
											bed5e858-10.80.58.44/D:ServiceType2/1516532769734/Put/vlen=6/seqid=0
											bed5e858-10.80.58.44/D:SiteName/1516532769734/Put/vlen=12/seqid=0
											bed5e858-10.80.58.44/D:SiteType1/1516532769734/Put/vlen=0/seqid=0
											bed5e858-10.80.58.44/D:SiteType2/1516532769734/Put/vlen=0/seqid=0
											bed5e858-10.80.58.44/D:SourceIP/1516532769734/Put/vlen=11/seqid=0
											bed5e858-10.80.58.44/D:SourcePort/1516532769734/Put/vlen=5/seqid=0
											bed5e858-10.80.58.44/D:UA/1516532769734/Put/vlen=0/seqid=0
											bed5e858-10.80.58.44/D:URL/1516532769734/Put/vlen=0/seqid=0
											bed5e858-10.80.58.44/D:UpPackNum/1516532769734/Put/vlen=1/seqid=0
											bed5e858-10.80.58.44/D:UpPayLoad/1516532769734/Put/vlen=2/seqid=0*/

											//System.out.println(cell.toString());
											System.out.println("======");
											//System.out.println("getQualifierArray:"+Bytes.toString(cell.getQualifierArray()));
										}
								}
						);

						//JavaRDD<String> javaRDD = hbaseRDD.map()

						logger.info("从HBase中Scan获得的条数："+hbaseRDD.count());}

			}catch (Exception e){
				logger.error("表："+tableName.toString()+"不存在...");
			}finally {
				jsc.stop();
			}
		}catch (Exception e){
			logger.error("实例化HbaseConntext失败..."+e.getMessage());
		}finally {
			jsc.stop();
		}

		return null;
	}

	public static class ResultFunction implements Function<Result, String> {

		private static final long serialVersionUID = 1L;

		public String call(Result result) throws Exception {
			Iterator<Cell> it = result.listCells().iterator();
			StringBuilder b = new StringBuilder();

			//通过getRow()方法，获得rowKey
			b.append(Bytes.toString(result.getRow())).append(":");

			while (it.hasNext()) {
				Cell cell = it.next();
				String q = Bytes.toString(cell.getQualifierArray());
				if (q.equals("counter")) {
					b.append("(")
							.append(Bytes.toString(cell.getQualifierArray()))
							.append(",")
							.append(Bytes.toLong(cell.getValueArray()))
							.append(")");
				} else {
					b.append("(")
							.append(Bytes.toString(cell.getQualifierArray()))
							.append(",")
							.append(Bytes.toString(cell.getValueArray()))
							.append(")");
				}
			}
			return b.toString();
		}
	}
}
