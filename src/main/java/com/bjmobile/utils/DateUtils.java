package com.bjmobile.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: qeekey
 * Date: 14-5-27
 * Time: 下午2:02
 * To change this template use File | Settings | File Templates.
 */
public class DateUtils {
	public static String FORMAT_DAY = "yyyyMMddHHmmss";
	public static String FORMAT_DAY_HMS = "yyyy-MM-dd HH:mm:ss";
	/**
	 * 日期转换成年月日的字符串
	 * @param date
	 * @return str
	 */
	public static String DateToStrYMD(Date date) {
		SimpleDateFormat format = new SimpleDateFormat(FORMAT_DAY);
		String str = format.format(date);
		return str;
	}

	/**
	 * 日期转换成年月日的字符串
	 * @param date
	 * @return str
	 */
	public static String DateToStrYMDHMS(Date date) {
		SimpleDateFormat format = new SimpleDateFormat(FORMAT_DAY_HMS);
		String str = format.format(date);
		return str;
	}

	/**
	 * 日期转换成年月日的字符串
	 * @param dateString
	 * @return str
	 */
	public static Date StrToDate(String  dateString, String format) {
		try{
			SimpleDateFormat sdf = new SimpleDateFormat(format);
			Date date = sdf.parse(dateString);
			return date;
		}catch (ParseException e){
			System.out.println(e.getMessage());
		}
		return null;
	}




}
