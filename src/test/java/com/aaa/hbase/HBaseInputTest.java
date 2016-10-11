package com.aaa.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Test;
import org.mortbay.log.Log;

import com.client.hdfs.common.HbaseRemoteConfig;
import com.client.hdfs.common.RemoteConfig;

public class HBaseInputTest {
	
	static final String inputPath = "/user/test/input/INTLCTRY/README_TITLE_SORT.txt";
	
	// Table Name
	public static final String TABLE_NAME = "INTLCTRY_TABLE";
	
	// Column Family
	public static final byte[] COLUMN_FAMILY = "INTLCTRY_DATA".getBytes();

	private HBaseAdmin admin;
	
	@Test
	public void testHBaseInputTest () throws Exception {
		
		Configuration conf = RemoteConfig.getConf();
		
		// 테이블 이름을 Configuration에 설정.
		conf.set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);
		
		conf = HbaseRemoteConfig.getConf(conf);
		
		Job job = Job.getInstance(conf, "HBase Data Insert");
		
		
		admin = new HBaseAdmin(conf);
		
		if (admin.tableExists(TABLE_NAME)) {
			
			Log.debug("Table is exist!");
			
			// 테이블 drop
			admin.disableTable(TABLE_NAME);
			admin.deleteTable(TABLE_NAME);
			
		}
		else {
			
			HTableDescriptor tableDesc 		= new HTableDescriptor(TableName.valueOf(TABLE_NAME));  // HBase의 Table에 대한 상세 정보를 포함하는 클래스. Table에 대한 속성 값을 관리.
			HColumnDescriptor columnDesc	= new HColumnDescriptor(COLUMN_FAMILY);					// HBase의 Column Family를 처리하고 관리하는 클래스
			
			tableDesc.addFamily(columnDesc);
			
			admin.createTable(tableDesc);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TableOutputFormat.class);
			
			job.setOutputKeyClass(ImmutableBytesWritable.class);
			job.setOutputValueClass(Put.class);
			
			job.setMapperClass(MyMapper.class);
			job.setNumReduceTasks(0);  // 이 unit test에서는 Mapper까지만 진행하므로 reduce task는 생성하지 않는다.
			
			FileInputFormat.setInputPaths(job, new Path(inputPath));
			
			job.setJarByClass(HBaseInputTest.class);
			
			job.waitForCompletion(true);
			
		}
		
	}

}
