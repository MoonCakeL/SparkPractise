package sparkHbase.example.utils;

import java.util.Arrays;

public class UsedUtils {

	public static void main(String[] args) throws Exception {


		/*HBaseUtils.put("hbaseHttp","bfba9614-10.80.58.00", "D", "BeginTime", "1368607184116");
		HBaseUtils.put("hbaseHttp","bfba9614-10.80.58.00", "D", "EndTime", "1368607184116");
		HBaseUtils.put("hbaseHttp","bfba9614-10.80.58.00", "D", "MSISDN", "5050");
	*/
       HBaseUtils.create("test", Arrays.asList("D1","D2").toArray().toString());

	}

}
