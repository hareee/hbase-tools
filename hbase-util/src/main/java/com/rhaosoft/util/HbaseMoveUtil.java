package com.rhaosoft.util;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseMoveUtil {
	private static Logger log = LoggerFactory.getLogger(HbaseMergeUtil.class);

	public static void main(String[] args) throws Exception {
		String env = args[0];
		Integer threshold = Integer.parseInt(args[1]);
		String namespace = null;
		if (args.length == 3) {
			namespace = args[2];
		}
		if (env == null) {
			env = "dev";
		}
		log.info("env:{},threshlold:{}, namespace:{}", env, threshold, namespace);
		AppConfig.loadenv(env);
		Connection conn = HbaseConn.getInstance();
		Admin admin = conn.getAdmin();
		//http://hadoop-134-84-69-7.anhui.chinatelecom.com:60010/jmx?qry=Hadoop:service=HBase,name=Master,sub=Server
		
		Collection<ServerName> regionservers = admin.getClusterStatus().getServers();
		for(ServerName rsQname : regionservers){
		    log.info("rsQname:{}", rsQname);
		}
		TableName[] tablenames;
		if(namespace==null) {
			tablenames = admin.listTableNames();
		} else {
			tablenames = admin.listTableNamesByNamespace(namespace);
		}
		// 遍历所有表
		for (int i = 0; i < tablenames.length; i++) {
			// 取到1张表，查询表的region分布，
			TableName tbname = tablenames[i];
			String qname = tbname.getNameWithNamespaceInclAsString();
			List<HRegionInfo> regionInfos = admin.getTableRegions(tablenames[i]);
			// 遍历所有region
			for (int j = 0; j < regionInfos.size(); j++) {
				HRegionInfo currentRegion = regionInfos.get(j);
				HRegionLocation location = MetaTableAccessor.getRegionLocation(conn, currentRegion);
				String hostname = location.getHostname();
				String encodedName = currentRegion.getEncodedName();
				log.info("qname:{}, hostnamae:{}, encodedName:{}", qname, hostname, encodedName);
			}
		}
		// move region，做到1、平均分配；2、分配到region最少的RS上
	}
}
