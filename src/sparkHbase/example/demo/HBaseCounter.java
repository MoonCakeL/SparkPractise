package sparkHbase.example.demo;

import java.io.IOException;

public class HBaseCounter  {
/*
	hbase(main):044:0> create 'countTest','D'
	Created table countTest
	Took 2.3286 seconds
	hbase(main):045:0> incr 'countTest','rowKey1','D:countNumber',1
	COUNTER VALUE = 1
	Took 0.0481 seconds
	hbase(main):046:0> incr 'countTest','rowKey1','D:countNumber',1
	COUNTER VALUE = 2
	Took 0.0099 seconds
	hbase(main):048:0> get_counter 'countTest','rowKey1','D:countNumber'
	COUNTER VALUE = 2
	Took 0.0056 seconds
	hbase(main):049:0> get 'countTest','rowKey1','D:countNumber'
	COLUMN                             CELL
	D:countNumber                     timestamp=1516604143638, value=\x00\x00\x00\x00\x00\x00\x00\x02
	1 row(s)
	Took 0.0081 seconds
	*/

	//单行计数器，作用与列簇中的列上，通过每次incr对该列簇上的列进行累计，可以是累计加1，也可是累计加10，或者-1
	//使用incr方法后，该列中的数据会自动添加在表中该列上，不能使用put方法进行累计操作
	//因为#使用了put去修改计数器 会导致后面的错误 原因是‘1‘会转换成Bytes.toBytes()

	//计数器的作用主要是做用于类似对于收集统计计数信息应用，类似页面的点击
	//每次点击一次就可以调用一次incr，然后就会根据后面的累计数字进行累计
	//通过get_counter可以获得总的累计数，每次incr后也会返回当前的最大累计数
	//除了单行计数器还有多行计数器。
	//对于incr可以理解为，对表中某个列簇下的某个列进行了一次累计+1的操作，可以知道这个列簇下的列
	//被"hits（点击）"了几次
	//计数器默认从0开始计数，累计后会自动"插入/更新"表中该累计列
	//API：在HTable类下Increment方法


	public static void main(String[] args) throws IOException {



		//TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf("countTest"));

		//Table table = HbaseConnectFactory.getInstance().getHbaseConnect().getTable(TableName.valueOf("countTest"));



	}

}
