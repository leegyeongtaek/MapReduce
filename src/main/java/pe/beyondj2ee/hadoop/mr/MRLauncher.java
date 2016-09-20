/* Copyright (c) 2013
 * All right reserved.
 * blog : http://beyondj2ee.wordpress.com
 * twitter : http://www.twitter.com/beyondj2ee
 * facebook : https://www.facebook.com/beyondj2ee
 * Revision History
 * Author              Date                         Description
 * ------------------   --------------                  ------------------
 *   beyondj2ee         2013. 1. 11
 */
package pe.beyondj2ee.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

public class MRLauncher {


	static final String FS_DEFAULT_NAME = "hdfs://10.203.7.203:9000";  // “.jar“를 배포할 “Name Node” 서버 정보

	static final String MAPRED_JOB_TRACKER = "hdfs://10.203.7.203:9001"; // “JobConf” 정보를 전송할 “Job Tracker” 서버 정보

	static final String HADOOP_USER = "131084,scom"; // HDFS는 아이디와 그룹으로 인증 (확인 명령어 : whoami / bash -c group)

	public int invokeMR(String jobName, String[] args) throws Exception {

		int res = 1;

		if (WordCount.class.getName().equals(jobName)) {

			Configuration conf = new Configuration();
			
			BasicConfigurator.configure();
			
//			System.setProperty("mapreduce.cluster.local.dir", "D:/hadoop-2.7.1");
			System.setProperty("hadoop.home.dir", "D:/HDFS/local/etc/hadoop");
			conf.set("fs.defaultFS", FS_DEFAULT_NAME);
			conf.set("mapreduce.jobtracker.address", MAPRED_JOB_TRACKER);
//			conf.set("yarn.resourcemanager.address", MAPRED_JOB_TRACKER);
//			conf.set("hadoop.job.ugi", HADOOP_USER);
			conf.setBoolean("dfs.client.use.datanode.hostname", true);	// IP address가 아닌 host name으로 client가 datanode를 처리함.
			

			// “ToolRunner” 오브젝트는 “Tool 인터페이스“를 구현한 클래스의 “run” 메서드 를 호출
			res = ToolRunner.run(conf, new WordCount(), args);
		}

		return res;
	}

}