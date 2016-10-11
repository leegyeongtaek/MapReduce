package com.client.hdfs.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.mortbay.log.Log;

import com.google.protobuf.ServiceException;

public class HbaseRemoteConfig {
	
	public static Configuration getConf(Configuration config) {
		
		Configuration conf = null;
		
		if (config == null) {
			// configuration
			conf = HBaseConfiguration.create();
		
		}
		else {
			conf = config;
		}
		
//		conf.clear();
		
		conf.set("hbase.master", "master:60000");
		conf.set("hbase.zookeeper.quorum", "master,slave1,slave3");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");

		try {
			HBaseAdmin.checkHBaseAvailable(conf);
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ServiceException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Log.debug("connected to hbase");
		
		return conf;
		
	}
	
}
