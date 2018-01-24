package sparkHbase.example.utils;

import HttpNetLog.util.DateUtil;

import java.util.Date;
import java.util.UUID;


/**
 * 提供唯一标识符
 *
 */
public class UUIDUtil {

	
	public static String getRowKey(){

		return UUID.randomUUID().toString()
				.replace("-", "")
				.substring(0,8) + "-" + DateUtil.formateDate(new Date(),"yyyyMMddHHmmss");
	}
	public static void main(String[] args) {

		System.out.println(UUIDUtil.getRowKey().length());
	}
}
