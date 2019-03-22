package com.rhaosoft.util;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HDFS常规操作
 *
 */
public class HdfsUtil {
	private static UserGroupInformation ugi;
	private static Logger log = LoggerFactory.getLogger(HbaseMergeUtil.class);

	
	public static void login(){
		Configuration conf = getConf();
		UserGroupInformation.setConfiguration(conf);
		//连接远程环境本机，需要设定
		try {
			UserGroupInformation.loginUserFromKeytab(AppConfig.PRINCIPAL, AppConfig.KEYTAB_LOCATE);
			ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(AppConfig.PRINCIPAL, AppConfig.KEYTAB_LOCATE);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static Configuration getConf(){
		Configuration config = new Configuration() ;
		config.set("fs.defaultFS", AppConfig.DEFAULT_FS);
		config.set("dfs.nameservices", AppConfig.NAMESERVICE);
		config.set("dfs.client.failover.proxy.provider.nameservice1","org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
		config.set("dfs.ha.namenodes.nameservice1", "namenode1,namenode2");
		config.set("dfs.namenode.rpc-address.nameservice1.namenode1", AppConfig.NAMENODE1);
		config.set("dfs.namenode.rpc-address.nameservice1.namenode2", AppConfig.NAMENODE2);
		config.set("hadoop.security.authentication", "Kerberos");
		config.set("dfs.namenode.kerberos.principal", AppConfig.HDFS_PRINCIPAL);
		return config;
	}

	
	public static long getHdfsDirSize(String path) {
		login();
		Configuration conf = getConf();
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(new URI(path), conf);
		} catch (Exception e) {
			log.error(e.getMessage());
		}
		try {
			long size = hdfs.getContentSummary(new Path(path)).getLength();
			long sizeInMB = size / 1024 / 1024;
			return sizeInMB;
		} catch (IOException e) {
			log.error(e.getMessage());
		} finally {
			try {
				hdfs.close();
			} catch (IOException e) {
			}
		}
		return 0;
	}
	
	
	public static void main(String[] args) {
		String path = "/hbase/data/oip/LOGINSTANCE";
		long size = getHdfsDirSize(path);
		System.out.println(size);
	}
}


