package HttpNetLog.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {

	/**
	 * @Title:getTime
	 * @Description:获取当前毫秒时间
	 * @return String
	 * @throws
	 */
	public static String getTime() {
		Date now = new Date();
		String timeStr = String.valueOf(now.getTime());
		return timeStr;
	}

	/**
	 * @Title:formateDate
	 * @Description:格式化时间
	 * @param fdate
	 * @param dateFormat
	 * @return
	 */
	public static String formateDate(Date fdate, String dateFormat) {
		SimpleDateFormat dataFormat = new SimpleDateFormat(dateFormat);
		String date = dataFormat.format(fdate);
		return date;
	}

	/**
	 * 把日期字符串转换为calendar对象
	 * 
	 * @param dateStr
	 *            日期字符串
	 * @param dateFormat
	 *            日期格式化类型，如"yyyy-MM-dd"
	 * @return calendar对象
	 */
	public static Calendar strToCalendar(String dateStr, String dateFormat) {
		Calendar calendar = null;
		// 声明一个Date类型的对象
		Date date = null;
		// 声明格式化日期的对象
		SimpleDateFormat format = null;
		if (dateStr != null) {
			// 创建日期的格式化类型
			format = new SimpleDateFormat(dateFormat);
			// 创建一个Calendar类型的对象
			calendar = Calendar.getInstance();
			// forma.parse()方法会抛出异常
			try {
				// 解析日期字符串，生成Date对象
				date = format.parse(dateStr);
				// 使用Date对象设置此Calendar对象的时间
				calendar.setTime(date);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return calendar;
	}
	
	/**
	 * @Title:getBeforeDay
	 * @Description:获取时间的前几天时间
	 * @param days
	 * @return
	 */
	public static Date getBeforeDay(Date date, int days){
		return getAfterDay(date,-days);
	}
  
	/**
	 * @Title:getAfterDay
	 * @Description:获取时间的后几天时间
	 * @param time
	 * @param days
	 * @return
	 */
	public static Date getAfterDay(Date date, int days){
		long times = date.getTime()  + days * 24 * 60 * 60 * 1000L;
		return new Date(times);
	}
}
