package com.rhaosoft.util;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 连接hbase新集群
 * @ClassName:
 * @Author:yuhan
 * @Since:JDK1.6
 * @date 2016年8月19日 上午9:55:54
 * @Version:1.1.0 Copyright 天源迪科合肥大数据中心 Corporation 2016 版权所有
 */
public class HbaseMergeUtil {
	private static Logger log = LoggerFactory.getLogger(HbaseMergeUtil.class);
	
	public static void main(String[] args) throws Exception {
		String env = args[0];
		Integer standard = Integer.parseInt(args[1]);
		String namespace = null;
		if(args.length==3) {
			namespace = args[2];
		}
		log.info("env:{},namespace:{}", env, namespace);
		if (env==null) {
			env = "dev";
		}
		AppConfig.loadenv(env);
		Connection conn = HbaseConn.getInstance();
		Admin admin = conn.getAdmin();
		String pattern = "^((?!clzx_opr).)*$";
		TableName[] tablename;
		if(namespace==null) {
			tablename = admin.listTableNames(pattern, false);
		} else {
			tablename = admin.listTableNamesByNamespace(namespace);
		}
		// 每100个合并语句执行一次
		for (int i = 0; i < tablename.length; i++) {
			List<HRegionInfo> hRegionInfo = admin.getTableRegions(tablename[i]);
			String qname = tablename[i].getNameAsString();
			String ns = tablename[i].getNamespaceAsString();
			// 不合并系统表
			if(ns.equals("default") || tablename[i].isSystemTable()) {
				continue;
			}
			String tbname = qname.split(":")[1];
			Integer regionAmount = hRegionInfo.size();
			// 获取表大小
			String path = "/hbase/data/" + ns + "/" + tbname ;
			long hdfsSize = HdfsUtil.getHdfsDirSize(path);
			Long avgSize = hdfsSize/regionAmount;
			log.info(String.format("%s:%s total size:%sMB avgsize:%sMB standard:%sMB regionAmount:%s" ,ns, tbname, hdfsSize, avgSize, standard, regionAmount));
			// 如avg_size < 128M and region数量 > 1, 执行合并region
			if(avgSize < standard && regionAmount > 1) {
				//merge region
				int mergeCount = hRegionInfo.size()/2;
				ExecutorService executor = Executors.newCachedThreadPool();
				CountDownLatch latch = new CountDownLatch(mergeCount);
				for (int j = 0;j < mergeCount; j++) {
					HRegionInfo first = hRegionInfo.get(j);
					HRegionInfo second = hRegionInfo.get(j+1);
					// 调用api执行merge语句
					MergeTask task = new MergeTask(admin, first, second, latch);
					executor.execute(task);
				}
				latch.await();
				executor.shutdown();
			}
		}
		admin.close();
		conn.close();
	}
}

class MergeTask implements Runnable {
	private Logger logger = LoggerFactory.getLogger(HbaseMergeUtil.class);
	private Admin admin;
	private HRegionInfo first;
	private HRegionInfo second;
	private CountDownLatch latch;
	
     
    public MergeTask(Admin admin, HRegionInfo first, HRegionInfo second, CountDownLatch latch) {
        this.admin = admin;
        this.first = first;
        this.second = second;
        this.latch = latch;
    }
     
    public void run() {
		try {
			logger.info("------------------------" + first.getEncodedName() + ',' + second.getEncodedName() + " merge begin");
			admin.mergeRegions(first.getEncodedNameAsBytes(), second.getEncodedNameAsBytes(), false);
			logger.info("------------------------" + first.getEncodedName() + ',' + second.getEncodedName() + " merge success");
		} catch (IOException e) {
			logger.info("------------------------" + first.getEncodedName() + ',' + second.getEncodedName() + " merge failed");
		} finally {
			this.latch.countDown();
		}
	}
}