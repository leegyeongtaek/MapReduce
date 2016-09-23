package com.aaa.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;

import com.aaa.datatype.CountryData;
import com.client.hdfs.common.RemoteConfig;

/**
 * 
 * @author 131084
 * 다중 출력 테스트
 * 
 */
public class MultiInputMapReduceTest {
	
	static final String inputPath1 = "/user/input/testData/INTLCTRY/README_TITLE_SORT.txt";
	static final String inputPath2 = "/user/input/testData/OECDTTL/README_TITLE_SORT.txt";
	static final String outputPath1 = "/user/output/INTLCTRY/multiInput_temp";
	static final String outputPath2 = "/user/output/INTLCTRY/multiInput_INTLCTRY";
	
	@Test
	public void testMultiOutputMapReduce () throws Exception {
		
		Configuration conf = RemoteConfig.getConf();
		
		/**
		 * Multiple mapper Data join
		 */
		Job job1 = Job.getInstance(conf, "Data Join");
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setReducerClass(ReducerJoin.class);
		
		job1.setOutputFormatClass(TextOutputFormat.class);

		MultipleInputs.addInputPath(job1, new Path(inputPath1), TextInputFormat.class, MultiInputMapper1.class);
		MultipleInputs.addInputPath(job1, new Path(inputPath2), TextInputFormat.class, MultiInputMapper2.class);
		
		FileOutputFormat.setOutputPath(job1, new Path(outputPath1));
		
		job1.setJarByClass(MultiInputMapReduceTest.class);
		
		job1.waitForCompletion(true);
		
		/**
		 * Multipe mapper Data join result using job
		 */
		Job job2 = Job.getInstance(conf, "Data Calculate");
		
		job2.setOutputKeyClass(CountryData.class);
		job2.setOutputValueClass(FloatWritable.class);
		
		job2.setMapperClass(CalcuMapper.class);
		job2.setReducerClass(CalcuReducer.class);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job2, new Path(outputPath1));
		FileOutputFormat.setOutputPath(job2, new Path(outputPath2));
		
		job2.setJarByClass(MultiInputMapReduceTest.class);
		
		job2.waitForCompletion(true);
		
		// 중간 출력 파일 삭제
		FileSystem hdfs = FileSystem.get(conf);
		Path path = new Path(outputPath1);
		
		if (hdfs.exists(path)) {
			hdfs.delete(path, true);
		}
	}
	
}
