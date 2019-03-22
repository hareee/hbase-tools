package com.rhaosoft.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.snapshot.ExportSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandUtil {
	
	private static Logger log = LoggerFactory.getLogger(CommandUtil.class);

	public static Integer callShell(String shellString) {
		Integer exitValue = -1;
		try {
			Process process = Runtime.getRuntime().exec(shellString);
			exitValue = process.waitFor();
			BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
			String line = "";
			while ((line = input.readLine()) != null) {
				log.info(line);
			}
			return exitValue;
		} catch (Throwable e) {
			log.error("call shell failed. " + e);
		}
		return exitValue;
	}
	
    public static String exec1(String command) {
        String returnString = "";
        Process pro = null;
        Runtime runTime = Runtime.getRuntime();
        if (runTime == null) {
            System.err.println("Create runtime failed!");
        }
        try {
            pro = runTime.exec(command);
            BufferedReader input = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            PrintWriter output = new PrintWriter(new OutputStreamWriter(pro.getOutputStream()));
            String line;
            while ((line = input.readLine()) != null) {
                returnString = returnString + line + "\n";
            }
            input.close();
            output.close();
            pro.destroy();
        } catch (IOException ex) {
        		ex.printStackTrace();
        }
        return returnString;
    }
    
	public static String exec(String cmd) {
		try {
			String[] cmdA = { "/bin/sh", "-c", cmd };
			Process process = Runtime.getRuntime().exec(cmdA);
			LineNumberReader br = new LineNumberReader(new InputStreamReader(process.getInputStream()));
			StringBuffer sb = new StringBuffer();
			String line;
			while ((line = br.readLine()) != null) {
				sb.append(line).append("\n");
			}
			return sb.toString();
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * 导出快照到备份集群
	 * @throws Exception
	 */
	private static void exportSnapshotByAPI(String snapshot) throws Exception {
		List<String> args = new ArrayList<String>();
		args.add("-snapshot");
		args.add(snapshot);
		args.add("-copy-from");
		args.add("hdfs://134.64.12.10:8020/hbase");
		args.add("-copy-to");
		args.add("hdfs://134.84.67.29:8020/hbase");
		args.add("-mappers");
		args.add("200");
		args.add("-bandwidth");
		args.add("1024");
		args.add("-chuser");
		args.add("hbase");
		args.add("-chgroup");
		args.add("hbase");	
		args.add("-chmod");
		args.add("777");	
		args.add("-overwrite");
		String[] array =new String[args.size()];
		AppConfig.loadenv("backup");
		Connection conn = HbaseConn.getInstance();
		Configuration conf = new Configuration();
		conf.addResource("/etc/hadoop/conf/yarn-site.xml");
		conf.addResource("/etc/hadoop/conf/mapred-site.xml");
		ExportSnapshot.main(args.toArray(array));
	}
	
	public static void main(String[] args) throws Exception {
		if(args[0].length() > 0) {
			String command = args[0];
		}
		String str = "hadoop jar  /opt/cloudera/parcels/CDH/jars/hadoop-mapreduce-examples-*.jar  pi  1 1 ";
		String exportStr = "hbase org.apache.hadoop.hbase.snapshot.ExportSnapshot \\\n" + 
				"-snapshot usi_odso-10000people-snapshot123 \\\n" + 
				"-copy-from hdfs://134.64.12.10:8020/hbase \\\n" + 
				"-copy-to hdfs://134.84.67.29:8020/hbase \\\n" + 
				"-mappers 200 \\\n" + 
				"-overwrite \\\n" + 
				"-bandwidth 1024 \\";
		System.out.println(CommandUtil.callShell(exportStr));
	}
}
