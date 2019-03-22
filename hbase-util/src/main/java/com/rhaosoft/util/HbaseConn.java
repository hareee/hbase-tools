package com.rhaosoft.util;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseConn {
	private static Logger log = LoggerFactory.getLogger(HbaseMergeUtil.class);
	private static org.apache.hadoop.conf.Configuration config = null;
	private static Connection conn = null;
	private static Admin admin = null;
	
	public static Admin getAdmin() {
		return admin;
	}
	
	public synchronized static Connection getInstance() {
		try {
			conn = null;
			config = null;
			if (conn == null || conn.isClosed() || conn.isAborted()) {
				if (config == null) {
					config = HBaseConfiguration.create();
				}
				//生产环境
				config.set("hbase.zookeeper.quorum", AppConfig.QUROM);
				config.set("kerberos.principal", AppConfig.PRINCIPAL);// 这个可以理解成用户名信息，也就是Principal
				//hbase/dsjpt014041
				config.set("hbase.master.kerberos.principal", AppConfig.HBASE_PRINCIPAL);
				config.set("hbase.regionserver.kerberos.principal", AppConfig.HBASE_PRINCIPAL);
				config.set("hbase.thrift.kerberos.principal", AppConfig.HBASE_PRINCIPAL);
				
				config.set("hbase.zookeeper.property.clientPort", AppConfig.ZKPORT);

				config.set("hbase.rootdir", "/hbase");
				config.set("zookeeper.znode.parent", "/hbase");
				// 这样我们就不需要交互式输入密码了
				config.set("keytab.file", AppConfig.KEYTAB_LOCATE);
				// 这个可以理解成用户名信息，也就是Principal
				config.set("hbase.security.authentication", "kerberos");
				config.set("hbase.rpc.engine", "org.apache.hadoop.hbase.ipc.SecureRpcEngine");
				config.set("hbase.security.authorization", "true");
				
				config.set("hadoop.security.authentication", "kerberos");
				//config.set("kerberos.principal", "hbase/_HOST@TESTCDH");
				config.set("hbase.client.retries.number", "3"); // 重试次数，默认为14，可配置为3
				config.set("zookeeper.recovery.retry", "3");// zk的重试次数，可调整为3次
				config.set("hbase.client.pause", "50"); // 重试的休眠时间，默认为1s，可减少，比如100ms
				//config.set("hbase.rpc.timeout", "2000"); // rpc的超时时间，默认60s，不建议修改，避免影响正常的业务
				config.set("hbase.client.operation.timeout", "60000"); // 客户端发起一次数据操作直至得到响应之间总的超时时间,数据操作类型包括get、append、increment、delete、put等
				config.set("hbase.client.scanner.timeout.period", "10000"); // scan查询时每次与server交互的超时时间，默认为60s，可不调整。
				config.set("zookeeper.recovery.retry.intervalmill", "50");// zk重试的休眠时间，默认为1s，可减少，比如：200ms
				// 通过keytab登录安全hbase
				UserGroupInformation.setConfiguration(config);
				try {
					UserGroupInformation.loginUserFromKeytab(AppConfig.PRINCIPAL, AppConfig.KEYTAB_LOCATE);
					conn = ConnectionFactory.createConnection(config);
					admin = conn.getAdmin();
				} catch (IOException e) {
					log.error("HbaseUtilNew HConnection error ", e);
					throw e;
				}
			}
		} catch (Exception e) {
			log.error("HbaseUtil New getInstance error ", e);
			System.exit(-1);
		}
		return conn;
	}
}
