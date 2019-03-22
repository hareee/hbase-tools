package com.rhaosoft.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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
		
	public static String getUUID32(){
		String uuid = UUID.randomUUID().toString().replace("-", "").toLowerCase();
		return uuid;
	}
}
