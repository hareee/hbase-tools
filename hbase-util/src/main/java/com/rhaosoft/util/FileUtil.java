package com.rhaosoft.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtil {
	private static Logger log = LoggerFactory.getLogger(FileUtil.class);

	public static void write(String file, String content) throws IOException {
		if(Files.exists(Paths.get(file))) {
			Files.write(Paths.get(file), content.getBytes(), StandardOpenOption.APPEND);
		} else {
			Files.write(Paths.get(file), content.getBytes());
		}
	}
	
	public static void clear(String file) throws IOException{
		Path path = Paths.get(file);
		if(Files.exists(Paths.get(file))) {
			Files.delete(path);
		}
	}
		
	public static String getUUID32(){
		String uuid = UUID.randomUUID().toString().replace("-", "").toLowerCase();
		return uuid;
	}
	
	//读取table.template模板文件，生成文件清单table.list
	//1、固定表；2、月表、3、日表
	public static void genTableList(String template) throws IOException {
		//读取模板文件
		String currentDir = System.getProperty("user.dir");
		Path path = Paths.get(currentDir + File.separator + template);
		String lineSeparator = System.getProperty("line.separator");
		StringBuilder sb = new StringBuilder();
		if(Files.exists(path)) {
			List<String> lines = Files.readAllLines(path);
			for(Iterator<String> it = lines.iterator();it.hasNext();) {
				String line = it.next();
				if(line.contains("#YYYYMMdd")) {
					String todayTable = line.replace("#YYYYMMdd", DateUtil.getToday("YYYYMMdd"));
					String yestodayTable = line.replace("#YYYYMMdd", DateUtil.getYestoday("YYYYMMdd"));
					sb.append(todayTable + lineSeparator).append(yestodayTable + lineSeparator);
				} else if(line.contains("#YYYYMM")) {
					String thisMonthTable = line.replace("#YYYYMM", DateUtil.getThisMonth("YYYYMM"));
					String lastMonthTable = line.replace("#YYYYMM", DateUtil.getLastMonth("YYYYMM"));
					sb.append(thisMonthTable + lineSeparator).append(lastMonthTable + lineSeparator);
				} else if(line.contains("#dd")) {
					String todayTable = line.replace("#dd", DateUtil.getToday("dd"));
					String yestodayTable = line.replace("#dd", DateUtil.getYestoday("dd"));
					sb.append(todayTable + lineSeparator).append(yestodayTable + lineSeparator);
				} else if(line.contains("#d")) {
					String todayTable = line.replace("#d", DateUtil.getToday("d"));
					String yestodayTable = line.replace("#d", DateUtil.getYestoday("d"));
					sb.append(todayTable + lineSeparator).append(yestodayTable + lineSeparator);	
				} else {
					sb.append(line + lineSeparator);
				}
			}
			String resultFilePath = template.replace(".tmpl", ".table");
			FileUtil.clear(resultFilePath);
			FileUtil.write(resultFilePath, sb.toString());
		}
	}
	
	public static void main(String[] args) throws IOException {
		String template = "clzx.tmpl";
		FileUtil.genTableList(template);
	}
}
