package com.rhaosoft.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseSnapShotUtil {
	
	private static Logger log = LoggerFactory.getLogger(HBaseSnapShotUtil.class);
	
	private static List<TableName> tableNames = new ArrayList<TableName>();
	
	private static String cluster;
	
	private static Integer SLEEP_TIME = 60;
	
	private static Integer THREAD_POOL_SIZE = 5;
	
	static {
		// 按行读取文件
		try {
			log.info("read tablelist from current dir!");
			FileInputStream inputStream = new FileInputStream("./tablelist");
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
			String str = null;
			while ((str = bufferedReader.readLine()) != null) {
				tableNames.add(TableName.valueOf(str.getBytes()));
			}
			inputStream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
		String env = args[0]; 
		if(args.length==2) {
			THREAD_POOL_SIZE = Integer.parseInt(args[1]);
		}
		log.info("env:{}", env);
		if (env.equalsIgnoreCase("snapshot")) {
			AppConfig.loadenv("prod");
			cluster = "prod";
			if(!AppConfig.PRINCIPAL.equalsIgnoreCase("hbase/bigdata012012@MYCDH")) {
				log.info("cluster error! no need to take snapshot at backup cluster!");
				return;
			}		
			log.info("----------------------------------------------------------------------------");
			log.info("take snapshot at prod cluster!下面休眠60秒！如集群信息有误请及时终止执行！");
			log.info("----------------------------------------------------------------------------");
			Thread.sleep(SLEEP_TIME*1000);
			takeSnapshot();
			return;
		} 
		if (env.equalsIgnoreCase("restore")) {
			AppConfig.loadenv("backup");
			cluster = "backup";
			if(AppConfig.PRINCIPAL.equalsIgnoreCase("hbase/bigdata012012@MYCDH")) {
				log.info("cluster error! can't restore snapshot at prod cluster!");
				return;
			}
			log.info("-----------------------------------------------------------------------------");
			log.info("restore snapshot at backup cluster!下面休眠60秒！如集群信息有误请及时终止执行！");
			log.info("-----------------------------------------------------------------------------");
			Thread.sleep(SLEEP_TIME*1000);
			batchDeleteSnaphsot();
			batchExportSnapshotByCommand();
			log.info("------------------------------批量导入完成,开始恢复快照!----------------------------");
			batchRestoreSnapshot();
			log.info("------------------------------快照恢复完成!");
			return;
		} 
	}
	
	/**
	 * 批量导出快照到备份集群
	 * @throws Exception
	 */
	private static void batchExportSnapshotByCommand() {
		log.info("table, total:" + tableNames.size());
		ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		Integer size = tableNames.size();
		CountDownLatch latch = new CountDownLatch(size);
		for (TableName tablename : tableNames) {
			String snapshot = tablename.getNameAsString().replaceAll(":", "-") + "-snapshot";
			log.info("export snapshot:" + snapshot + " to " + cluster);
			// 调用api执行export语句
			ExportTask task = new ExportTask(tablename, latch);
			executor.execute(task);
		}
		try {
			latch.await(300, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			log.error(e.getMessage());
		} finally {
			executor.shutdown();
		}
	}
	
	/**
	 * @throws IOException
	 */
	public static void batchDeleteSnaphsot() throws IOException {
		Admin admin = HbaseConn.getInstance().getAdmin();
		List<SnapshotDescription> snapshots = admin.listSnapshots();
		for (SnapshotDescription snapshot : snapshots) {
			log.info("drop snapshot:" + snapshot + " from " + cluster);
			admin.deleteSnapshot(snapshot.getName());
		}
	}
	
	/**
	 * 导出快照到备份集群
	 * @param tablename
	 */
	public static void exportSnapshotByCommand(TableName tablename) {
		String snapshot = tablename.getNameAsString().replaceAll(":", "-") + "-snapshot";
		log.info("export snapshot:" + snapshot + " to " + cluster);
		StringBuffer sb =  new StringBuffer();
		sb.append("hbase org.apache.hadoop.hbase.snapshot.ExportSnapshot ")
			.append("-snapshot " + snapshot + " ")
			.append("-copy-from hdfs://134.64.12.11:8020/hbase ")
			.append("-copy-to hdfs://134.84.67.29:8020/hbase ")
			.append("-mappers 200 ")
			.append("-overwrite ")
			.append("-bandwidth 1024 ");
		String command = sb.toString();
		log.info("提交作业的脚本:" + command);
		Integer result = CommandUtil.callShell(command);
		log.info("mapreduce执行结果：" + result);
		if (result == 0) {
			log.info("export snapshot:" + snapshot + " to " + cluster + " success!");
		} else {
			log.error("export snapshot:" + snapshot + " to " + cluster + " failed!");
		}
	}
	
	/**
	 * 修复/hbase/.hbase-snapshot目录权限，默认属主无写与执行权限
	 */
	public static void repairHdfsPrivileges() {
		String command1="hadoop fs -chmod -R 777 /hbase/.hbase-snapshot";
		String command2="hadoop fs -chmod -R 777 /hbase/archive";
		CommandUtil.exec(command1);
		CommandUtil.exec(command2);
		log.info("repair .hbase-snapshot hdfs privileges success!");
	}
	
	private static void batchRestoreSnapshot() throws IOException {
		for (TableName tablename : tableNames) {
			restoreSnapshot(tablename);
		}
	}

	/**
	 * 在目标集群还原快照
	 * @param env
	 * @throws IOException 
	 * @throws Exception
	 */
	private static void restoreSnapshot(TableName tablename) throws IOException {
		Admin admin = HbaseConn.getInstance().getAdmin();
		String snapshotName = tablename.getNameAsString().replace(":", "-") + "-snapshot";
		boolean exist = admin.tableExists(tablename);
		if (exist) {
			log.info("table:" + tablename + " exists! drop first!");
			if (admin.isTableEnabled(tablename)) {
				log.info("table:" + tablename.getNameAsString() + "is enabled!, disable first!");
				// disable table;
				try {
					admin.disableTable(tablename);
					admin.deleteTable(tablename);
				} catch (IOException e) {
					log.error(e.getMessage());
				}
			}
			// restore snapshot;
			log.info("还原快照：" + snapshotName);
			try {
				//Builder builder = SnapshotDescription.newBuilder();
				//builder.setName(snapshotName);
				//SnapshotDescription desc = builder.build();
				//if(admin.isSnapshotFinished(desc)) {
					admin.restoreSnapshot(snapshotName);
				//} else {
				//	log.info("snapshot:" + snapshotName + " not finished!");
				//}
			} catch (IOException e) {
				log.error(e.getMessage());
				log.error("还原快照失败：" + snapshotName);
			}
			// enable table;
			log.info("enable:" + tablename.getNameAsString());
			try {
				admin.enableTable(tablename);
			} catch (IOException e) {
				log.error(e.getMessage());
			}
		} else {
			log.info("table:" + tablename + " not exists!");
			// clone snapshot;
			log.info("clone快照：" + snapshotName + " to table:" + tablename.getNameAsString());
			try {
				admin.cloneSnapshot(snapshotName, tablename);
			} catch (IOException e) {
				log.error(e.getMessage());
				log.error("克隆快照失败：" + snapshotName);
			}
		}
	}

	/**
	 * 在源集群生成快照
	 * @param env
	 * @throws Exception
	 */
	public static void takeSnapshot(){
		Connection conn = HbaseConn.getInstance();
		Admin admin = null;
		try {
			admin = conn.getAdmin();
		} catch (IOException e) {
			log.error("获取HBaseAdmin失败，程序退出！");
			System.exit(-1);
		}
		//读取表清单
		List<TableName> tableNames = new ArrayList<TableName>();
		//按行读取文件
		FileInputStream inputStream = null;
		try {
			inputStream = new FileInputStream("./tablelist");
		} catch (FileNotFoundException e) {
			log.error("tablelist文件不存在，程序退出!");
			System.exit(-2);
		}
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
		String str = null;
		try {
			while((str = bufferedReader.readLine()) != null){
				tableNames.add(TableName.valueOf(str.getBytes()));
			}
		} catch (IllegalArgumentException | IOException e) {
			log.error("读取tablelist文件失败，程序退出!");
			System.exit(-3);
		} finally {
			try {
				bufferedReader.close();
				inputStream.close();
			} catch (IOException e) {
				log.error(e.getMessage());
			}
		}
		List<SnapshotDescription> snapshots = new ArrayList<SnapshotDescription>();
		try {
			snapshots = admin.listSnapshots();
		} catch (IOException e) {
			log.error(e.getMessage());
			log.error("查询所有快照失败,程序退出！");
			System.exit(-4);
		}
		Set<String> snapshotSet = new HashSet<String>();
		// 取出已存在的快照集合，用来判断是否需要删除快照
		for(SnapshotDescription snapshot : snapshots) {
			snapshotSet.add(snapshot.getName());
		}
		log.info("快照集合大小：" + snapshotSet.size());
		// 删除已经存在的快照，并做新的快照
		for(TableName tablename : tableNames) {
			String snapshotName = tablename.getNameAsString().replace(":", "-") + "-snapshot";
			if(snapshotSet.contains(snapshotName)) {
				log.info("删除快照：" + snapshotName);
				try {
					admin.deleteSnapshot(snapshotName);
				} catch (IOException e) {
					log.error("删除快照"+ snapshotName + "失败!");
					log.error(e.getMessage());
				}
			}
			log.info("表:" + tablename + "生成快照：" + snapshotName);
			try {
				admin.snapshot(snapshotName, tablename);
			} catch (IllegalArgumentException | IOException e) {
				log.error("生成快照"+ snapshotName + "失败!");
				log.error(e.getMessage());
				throw new RuntimeException(e.getMessage());
			}
		}
	}
}

class ExportTask implements Runnable {
	private Logger logger = LoggerFactory.getLogger(ExportTask.class);
	private TableName tablename;
	private CountDownLatch latch;
	
	public ExportTask(TableName tablename, CountDownLatch latch){
		this.tablename = tablename;
		this.latch = latch;
	}
	
	@Override
	public void run() {
		try {
			logger.info("------------------------" + tablename.getNameAsString() + " export begin");
			HBaseSnapShotUtil.exportSnapshotByCommand(tablename);
			logger.info("------------------------" + tablename.getNameAsString() + " export success");
		} catch (Exception e) {
			logger.info("------------------------" + tablename.getNameAsString() + " export failed");
		} finally {
			this.latch.countDown();
		}	
	}
}
