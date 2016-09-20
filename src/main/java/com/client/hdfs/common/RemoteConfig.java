package com.client.hdfs.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.BasicConfigurator;

public class RemoteConfig {
	
	static final String FS_DEFAULT_NAME = "hdfs://10.203.7.203:9000";  // “.jar“를 배포할 “Name Node” 서버 정보

	static final String MAPRED_JOB_TRACKER = "hdfs://10.203.7.203:9001"; // “JobConf” 정보를 전송할 “Job Tracker” 서버 정보

	public static Configuration getConf() {
		
		Configuration conf = new Configuration();
		
		BasicConfigurator.configure();
		
		System.setProperty("hadoop.home.dir", "D:/HDFS/local/etc/hadoop");
		
		conf.set("fs.defaultFS", FS_DEFAULT_NAME);
		conf.set("mapreduce.jobtracker.address", MAPRED_JOB_TRACKER);
		conf.setBoolean("dfs.client.use.datanode.hostname", true);		// IP address가 아닌 host name으로 client가 datanode를 처리함.
		
		return conf;
		
	}
	
}
