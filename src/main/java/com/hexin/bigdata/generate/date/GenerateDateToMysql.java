package com.hexin.bigdata.generate.date;

import java.io.IOException;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.facebook.presto.jdbc.internal.jackson.databind.JsonNode;
import com.facebook.presto.jdbc.internal.jackson.databind.ObjectMapper;
import com.facebook.presto.jdbc.internal.joda.time.DateTime;
//import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.StringUtils;
//import org.joda.time.DateTime;
import org.jsoup.Connection.Response;
import org.jsoup.Jsoup;
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;

public class GenerateDateToMysql {
	// 百度 日历 的接口
	private static final String holidayUrl = "https://sp0.baidu.com/8aQDcjqpAAV3otqbppnN2DJv/api.php?query={0}&co=&resource_id=6018&ie=utf8&oe=utf8";
	private static final String[] NUMBERS = { "", "一", "二", "三", "四", "五", "六", "七", "八", "九" };
	private static final String[] WEEKS = { "", "一", "二", "三", "四", "五", "六", "日" };
	private static final Map<String, String> holidays = new HashMap<String, String>();
	private static final Set<String> buban = new HashSet<String>();

	private static void generateDatas(DateTime begin, DateTime end) throws ClassNotFoundException, SQLException{
		String url = "jdbc:mysql://172.20.10.39:3306/report_tmp?useUnicode=true&amp;characterEncoding=utf-8";
        String user = "admin";
        String password = "kpt7Ki5HZr5g";
        //1.加载驱动程序
        Class.forName("com.mysql.jdbc.Driver");
        //2.获得数据库链接
        Connection conn = DriverManager.getConnection(url, user, password);
        
        //先清空表中数据
        String delete_sql = "truncate table dim_public_date";
        PreparedStatement psts = conn.prepareStatement(delete_sql);
        psts.executeUpdate();
        
        // 设置手动提交  
        conn.setAutoCommit(false);
        String insert_sql = "replace into dim_public_date (s_date,s_date_name,year,year_name,year_start_date,year_end_date,is_year_end,half_year,half_year_name,half_year_start_date,half_year_end_date,is_half_year_end,quarter,quarter_name,quarter_start_date,quarter_end_date,is_quarter_end,month,month_name,month_start_date,month_end_date,is_month_end,week,week_name,in_week,in_week_name,week_start_date,week_end_date,is_week_end,is_workdate,pre_workdate,next_workdate,is_weekend,is_holiday,holiday_name,year_count,half_year_count,quarter_count,month_count) ";
        insert_sql = insert_sql + "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        psts = conn.prepareStatement(insert_sql);  
        
		begin = begin.withMillisOfDay(0);
		end = end.withMillisOfDay(0);

		while (begin.isBefore(end)) {
			//s_date 日期
			psts.setDate(1, new java.sql.Date(begin.toDate().getTime()));
			//s_date_name 日期名(yyyy年mm月dd日)
			psts.setString(2, begin.toString("yyyy年MM月dd日"));  
			//year 年号(yyyy)
			psts.setInt(3, begin.getYear());
			//year_name 年名称(yyyy年)
			psts.setString(4, begin.getYear() + "年");
			
			//year_start_date 年开始日期(yyyyMMdd)
			psts.setDate(5, new java.sql.Date(begin.dayOfYear().withMinimumValue().toDate().getTime()));
			DateTime lastDay = begin.dayOfYear().withMaximumValue();
			//year_end_date 年结束日期(yyyyMMdd)
			psts.setDate(6, new java.sql.Date(lastDay.toDate().getTime()));
			//is_year_end 是否年结束日(0：否；1：是)
			psts.setInt(7, begin.equals(lastDay) ? 1 : 0);
			
			int monthOfYear = begin.getMonthOfYear();
			DateTime halfF = begin.dayOfYear().withMinimumValue();
			if (monthOfYear > 6)
				halfF = halfF.withMonthOfYear(7);
			DateTime halfL = halfF.plusMonths(6).minusDays(1);
			//half_year 半年编号(yyyy1/2)
			psts.setString(8, begin.getYear() + (monthOfYear <= 6 ? "1" : "2"));
			//half_year_name 半年名称(上/下半年)
			psts.setString(9, monthOfYear <= 6 ? "上半年" : "下半年");
			
			//half_year_start_date 半年开始日期(yyyyMMdd)
			psts.setDate(10, new java.sql.Date(halfF.toDate().getTime()));
			//half_year_end_date 半年结束日期(yyyyMMdd)
			psts.setDate(11, new java.sql.Date(halfL.toDate().getTime()));
			//is_half_year_end 是否半年结束日期(0：否；1：是)
			psts.setInt(12, begin.equals(halfL) ? 1 : 0);
			
			int quart = begin.getMonthOfYear() / 3 + (begin.getMonthOfYear() % 3 > 0 ? 1 : 0);
			//quarter 季编号(yyyyq)
			psts.setInt(13, begin.getYear() * 10 + quart);
			//quarter_name 季名称(yyyy年q季)
			psts.setString(14, begin.toString("yyyy年") + quart + "季");
			DateTime quartF = begin.withMonthOfYear(quart * 3 - 2).withDayOfMonth(1);
			DateTime quartL = quartF.plusMonths(3).minusDays(1);
			
			//quarter_start_date 季开始日期(yyyyMMdd)
			psts.setDate(15, new java.sql.Date(quartF.toDate().getTime()));
			//quarter_end_date 季结束日期(yyyyMMdd)
			psts.setDate(16, new java.sql.Date(quartL.toDate().getTime()));
			//is_quarter_end 是否季结束日(0：否；1：是)
			psts.setInt(17, begin.equals(quartL) ? 1 : 0);
			
			//month 月编号(yyyymm)
			psts.setInt(18, Integer.valueOf(begin.toString("yyyyMM")));
			//month_name 月名称(yyyy年mm月)
			psts.setString(19, begin.toString("yyyy年MM月"));
			//month_start_date 月开始日期(yyyyMMdd)
			psts.setDate(20, new java.sql.Date(begin.dayOfMonth().withMinimumValue().toDate().getTime()));
			DateTime monthL = begin.dayOfMonth().withMaximumValue();
			
			//month_end_date 月结束日期(yyyyMmdd)
			psts.setDate(21, new java.sql.Date(monthL.toDate().getTime()));
			//is_month_end 是否月结束日(0：否；1：是)
			psts.setInt(22, begin.equals(monthL) ? 1 : 0);
			//week 周编号(eg：3)
			psts.setInt(23, begin.getWeekOfWeekyear());
			
			//week_name 周名称(eg：第三周)
			psts.setString(24, calcWeek(begin.getWeekOfWeekyear()));
			//in_week 星期几编号(eg：3)
			psts.setInt(25, begin.getDayOfWeek());
			//in_week_name 星期几名称(eg：星期三)
			psts.setString(26, "星期" + WEEKS[begin.getDayOfWeek()]);
			
			//week_start_date 周开始日期(yyyymmdd)
			psts.setDate(27, new java.sql.Date(begin.dayOfWeek().withMinimumValue().toDate().getTime()));
			//week_end_date 周结束日期(yyyyMmdd)
			psts.setDate(28, new java.sql.Date(begin.dayOfWeek().withMaximumValue().toDate().getTime()));
			//is_week_end 是否周结束日(0：否；1：是)
			psts.setInt(29, begin.getDayOfWeek() == 7 ? 1 : 0);
			//is_workdate 是否工作日(0：否；1：是)
			psts.setInt(30, isWorkDay(begin) ? 1 : 0);
			
			DateTime temp = begin;
			while (true) {
				temp = temp.minusDays(1);
				if (isWorkDay(temp)) {
					//pre_workdate
					psts.setDate(31, new java.sql.Date(temp.toDate().getTime())); // 上一工作日(yyyyMMdd)
					break;
				}
			}
			temp = begin;
			while (true) {
				temp = temp.plusDays(1);
				if (isWorkDay(temp)) {
					psts.setDate(32, new java.sql.Date(temp.toDate().getTime())); // 下一工作日(yyyyMMdd)
					break;
				}
			}
			//is_weekend 是否周末(0：否；1：是)
			psts.setInt(33, begin.getDayOfWeek() >= 6 ? 1 : 0);
			String hname = holidays.get(begin.toString("yyyy-M-d"));
			//is_holiday 是否法定节假日(0：否；1：是)
			psts.setInt(34, hname != null ? 1 : 0);
			//holiday_name 节假日名称(eg：春节、元旦)
			psts.setString(35, StringUtils.defaultIfEmpty(hname, ""));
			
			//year_count 到目前为止，年天数
			psts.setInt(36, begin.getDayOfYear());
			//half_year_count 到目前为止，半年天数
			psts.setInt(37, begin.getDayOfYear() - halfF.getDayOfYear() + 1);
			//quarter_count 到目前为止，季天数
			psts.setInt(38, begin.getDayOfYear() - quartF.getDayOfYear() + 1);
			//month_count 到目前为止，月天数
			psts.setInt(39, begin.getDayOfMonth());
			
			// 加入批量处理  
            psts.addBatch();          
				
            begin = begin.plusDays(1);
		}
		
		// 执行批量处理  
		psts.executeBatch(); 
		// 手动提交  
		conn.commit();
        conn.close(); 

	}

	private static String calcWeek(int week) {
		if (week < 10)
			return "第" + NUMBERS[week] + "周";
		if (week < 20)
			return "第十" + NUMBERS[week % 10] + "周";
		return "第" + NUMBERS[week / 10] + "十" + NUMBERS[week % 10] + "周";
	}

	private static boolean isWorkDay(DateTime dt) {
		String str = dt.toString("yyyy-M-d");
		if (buban.contains(str))
			return true;
		if (holidays.get(str) != null)
			return false;
		return dt.getDayOfWeek() < 6;
	}

	private static void spiderHoliday(DateTime begin, DateTime end) throws IOException {
		while (begin.isBefore(end)) {
			Response resp = Jsoup.connect(MessageFormat.format(holidayUrl, URLEncoder.encode(begin.toString("yyyy年MM月"), "utf-8")))
					.ignoreContentType(true).execute();
			JsonNode jsonNode = new ObjectMapper().readTree(resp.body()).get("data").get(0).get("holiday");
			if (jsonNode != null) {
				if (jsonNode.isArray()) {
					for (int i = 0; i < jsonNode.size(); i++) {
						spiderHoliday(jsonNode.get(i));
					}
				} else {
					spiderHoliday(jsonNode);
				}
			}

			begin = begin.plusMonths(1);
		}
	}

	private static void spiderHoliday(JsonNode n) {
		String name = n.get("name").asText();
		JsonNode jsonNode2 = n.get("list");
		for (int j = 0; j < jsonNode2.size(); j++) {
			JsonNode jn = jsonNode2.get(j);
			String date = jn.get("date").asText();
			String status = jn.get("status").asText();
			if ("1".equals(status)) {
				holidays.put(date, name);
			} else {
				buban.add(date);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		//开始时间
		DateTime begin = new DateTime("2013-01-01");
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar calendar = Calendar.getInstance();   
		int currentYear = calendar.get(Calendar.YEAR);
        calendar.clear();   
        calendar.set(Calendar.YEAR, (currentYear+1));
        java.util.Date currYearLast = calendar.getTime(); 
        //结束时间：取当前年份的下一年的一月一号
		DateTime end = new DateTime(sdf.format(currYearLast));
		//end = new DateTime("2020-01-01");
		
		spiderHoliday(begin, end);
		generateDatas(begin, end);
	}

}
