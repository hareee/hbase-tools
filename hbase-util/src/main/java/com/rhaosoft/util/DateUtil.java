package com.rhaosoft.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {
	public static String getYear(String pattern) {
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		Calendar c = Calendar.getInstance();
		c.setTime(new Date());
		Date time = c.getTime();
		String year = format.format(time);
		return year;
	}	
	public static String getToday(String pattern) {
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		Calendar c = Calendar.getInstance();
		c.setTime(new Date());
		Date time = c.getTime();
		String day = format.format(time);
		if ("#d".equalsIgnoreCase(pattern)) {
			day = day.replace("0", "");
		}
		return day;
	}
	
	public static String getYestoday(String pattern) {
		SimpleDateFormat format = new SimpleDateFormat(pattern);
        Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        c.add(Calendar.DATE, -1);
        Date time = c.getTime();
        String day = format.format(time);
		if ("#d".equalsIgnoreCase(pattern)) {
			day = day.replace("0", "");
		}
		return day;	
	}
	
	public static String getThisMonth(String pattern) {
		SimpleDateFormat format = new SimpleDateFormat(pattern);
        Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        Date d = c.getTime();
        String month = format.format(d);
        return month;
	}
	
	public static String getLastMonth(String pattern) {
		SimpleDateFormat format = new SimpleDateFormat(pattern);
        Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        c.add(Calendar.MONTH, -1);
        Date d = c.getTime();
        String day = format.format(d);
        return day;
	}
	
	public static void main(String[] args) {
		System.out.println(DateUtil.getToday("dd"));
		System.out.println(DateUtil.getYestoday("d"));
		System.out.println(DateUtil.getThisMonth("YYYYMM"));
		System.out.println(DateUtil.getLastMonth("YYYYMMdd"));
	}
}
