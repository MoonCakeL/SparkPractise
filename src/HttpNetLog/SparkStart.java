package HttpNetLog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkStart {

	private static final Logger looger = LoggerFactory.getLogger(SparkStart.class);

	public static void main(String[] args) throws Exception {

		//looger.info("SparkAppliction 启动,开启SparkStreaming");
		//HttpDataStreamSchema.StreamSpark();
		HttpDataToHbaseSchema.hbaseGo();


	}

}