package com.rhaosoft.util;

import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppConfig {
	public static String KEYTAB_LOCATE;
	public static String DEFAULT_FS;
	public static String NAMESERVICE;
	public static String NAMENODE1;
	public static String NAMENODE2;
	public static String PRINCIPAL;
	public static String HDFS_PRINCIPAL;
	public static String HBASE_PRINCIPAL;
	public static String QUROM;
	public static String ZKPORT;
	
	private static Logger log = LoggerFactory.getLogger(AppConfig.class);
	
	public static void loadenv(String env) {
		Properties p = new Properties();
		try {
			log.info("++++++load {} properties++++++++", env);
			if(env.equalsIgnoreCase("prod")) {
				p.load(AppConfig.class.getResourceAsStream("/prod.properties"));
			} else if(env.equalsIgnoreCase("backup")) {
				p.load(AppConfig.class.getResourceAsStream("/backup.properties"));
			} else {
				p.load(AppConfig.class.getResourceAsStream("/dev.properties"));
			}
			log.info(p.toString());
			KEYTAB_LOCATE = p.getProperty("keytab");
			DEFAULT_FS = p.getProperty("defaultfs");
			NAMESERVICE = p.getProperty("nameservices");
			NAMENODE1 = p.getProperty("namenode1");
			NAMENODE2 = p.getProperty("namenode2");
			PRINCIPAL = p.getProperty("principal");
			HDFS_PRINCIPAL = p.getProperty("hdfs.principal");
			HBASE_PRINCIPAL = p.getProperty("hbase.principal");
			QUROM = p.getProperty("quorum");
			ZKPORT = p.getProperty("zkport");
		} catch (IOException e) {
			log.error(e.getMessage());
		}
	}
	
	public static void main(String[] args) {
		AppConfig config = new AppConfig(); 
	}
}
