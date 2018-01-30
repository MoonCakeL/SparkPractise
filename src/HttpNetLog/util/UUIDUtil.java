package HttpNetLog.util;

import java.util.UUID;


/**
 * 提供唯一标识符
 *
 */
public class UUIDUtil {

	
	public static String getRowKeyUUID(){

		return UUID.randomUUID().toString()
				.replace("-", "")
				.substring(0,8) + "-";
		//+ DateUtil.formateDate(new Date(),"yyyyMMddHHmmss");
	}
	public static void main(String[] args) {

		System.out.println(UUIDUtil.getRowKeyUUID()+ " length: " +UUIDUtil.getRowKeyUUID().length());
	}
}
