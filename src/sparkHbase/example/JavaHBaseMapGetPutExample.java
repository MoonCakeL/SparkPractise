/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sparkHbase.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This is a simple example of using the foreachPartition
 * method with a HBase connection
 */
final public class JavaHBaseMapGetPutExample {

  private JavaHBaseMapGetPutExample() {}

  public static void main(String[] args) {
    if (args.length < 1) {
      System.out.println("JavaHBaseBulkGetExample {tableName}");
      return;
    }

    final String tableName = args[0];

    SparkConf sparkConf = new SparkConf().setAppName("JavaHBaseBulkGetExample " + tableName);
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    try {
      List<byte[]> list = new ArrayList<>(5);
      list.add(Bytes.toBytes("1"));
      list.add(Bytes.toBytes("2"));
      list.add(Bytes.toBytes("3"));
      list.add(Bytes.toBytes("4"));
      list.add(Bytes.toBytes("5"));

      JavaRDD<byte[]> rdd = jsc.parallelize(list);
      Configuration conf = HBaseConfiguration.create();

      JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

      hbaseContext.foreachPartition(rdd,
              new VoidFunction<Tuple2<Iterator<byte[]>, Connection>>() {
        public void call(Tuple2<Iterator<byte[]>, Connection> t)
                throws Exception {
          Table table = t._2().getTable(TableName.valueOf(tableName));
          //hbase.client.write.buffer
			// Default
			//2097152 2M
			/*客户端写缓存 默认2M
			BufferedMutator写入缓冲区的默认大小（以字节为单位）。
			一个更大的缓冲区需要更多的内存 - 在客户端和服务器端，
			因为服务器实例化传递的写入缓冲区来处理它 - 但是更大的缓冲区大小减少了RPC的数量。
			对于估计使用的服务器端内存，
			评估hbase.client.write.buffer * hbase.regionserver.handler.count*/

          BufferedMutator mutator = t._2().getBufferedMutator(TableName.valueOf(tableName));

          while (t._1().hasNext()) {
            byte[] b = t._1().next();
            Result r = table.get(new Get(b));
            if (r.getExists()) {
              mutator.mutate(new Put(b));
            }
          }

          mutator.flush();
          mutator.close();
          table.close();
        }
      });
    } finally {
      jsc.stop();
    }
  }

  public static class GetFunction implements Function<byte[], Get> {
    private static final long serialVersionUID = 1L;
    public Get call(byte[] v) throws Exception {
      return new Get(v);
    }
  }
}
