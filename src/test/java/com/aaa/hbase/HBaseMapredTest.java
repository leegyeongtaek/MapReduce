package com.aaa.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import com.client.hdfs.common.HbaseRemoteConfig;
import com.client.hdfs.common.RemoteConfig;

public class HBaseMapredTest {
	
	// Table Name
	public static final String TABLE_NAME = "INTLCTRY_TABLE";
	
	// Column Family
	public static final byte[] COLUMN_FAMILY = "INTLCTRY_DATA".getBytes();

	@Test
	public void testHBaseInputTest () throws Exception {
		
		Configuration conf = RemoteConfig.getConf();
		
		conf = HbaseRemoteConfig.getConf(conf);
		
		Job job = Job.getInstance(conf, "HBase MapReduce Test");
		
		
		job.setJarByClass(HBaseMapredTest.class);
		
		Scan scan = new Scan();
		scan.setRaw(true); // 디폴트는 false로 지정되어 있다.  false값에서 최신 버전만 반환된다. true는 전체 반환
		scan.setCaching(1000); // 캐쉬되는 row 수. 디폴트로 1 로 설정되어 있다.
		scan.setCacheBlocks(false);  // Scan 인스턴스에 Block의 캐쉬 여부를 설정. MapReduce작업을 위해 false로 지정한다.
		scan.setMaxVersions(); // Scan의 Version값을 최대값으로 설정하거나 범위를 지정한다. 최대 버전까지 범위로 지정 됨.
		
		// Filter나 기타 속성을 지정한다.
		Filter filterTitle = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("Brazil©,Active Population"));
		scan.setFilter(filterTitle);
		
		// Mapper 설정
		TableMapReduceUtil.initTableMapperJob(
				"INTLCTRY_TABLE", 		// input table 이름
				scan,         	       // 앞에서 설정한 Scan 객체를 넘긴다. 이 Scan 값은 Mapper 클래스의 map 함수의 Result 값에 담긴다.
				HBaseMapper.class,     // mapper class
				Text.class,            // mapper output key
				FloatWritable.class,   // mapper output value
				job);
		
		// Reducer 설정
		TableMapReduceUtil.initTableReducerJob(
				"INTLCTRY_TABLE",      // output table 이름
				HBaseReducer.class,     // reducer class
				job);
		
		job.setNumReduceTasks(1);
		
		job.waitForCompletion(true);
		
	}

}
