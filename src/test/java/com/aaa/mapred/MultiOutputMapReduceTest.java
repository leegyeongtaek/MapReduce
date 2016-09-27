package com.aaa.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
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
public class MultiOutputMapReduceTest {
	
	static final String inputPath = "/user/test/input/INTLCTRY/README_TITLE_SORT.txt";
	static final String outputPath = "/user/output/INTLCTRY/multiOutput_INTLCTRY4";
	
	@Test
	public void testMultiOutputMapReduce () throws Exception {
		
		Configuration conf = RemoteConfig.getConf();
		
		Job job = Job.getInstance(conf, "Multiple output");
		
		job.setOutputKeyClass(CountryData.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(MultiOutputMapper.class);
		job.setReducerClass(MultiOutputReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);

		// MultipleOutputs 클래스를 통해 기본 단일 출력 part-r-00000 를 여러 출력으로 output name을 설정하여 출력 
		// parameter(job, output name, output format class, output key class, output value class)
		MultipleOutputs.addNamedOutput(job, "Australia", TextOutputFormat.class, CountryData.class, FloatWritable.class);
		MultipleOutputs.addNamedOutput(job, "Brazil", TextOutputFormat.class, CountryData.class, FloatWritable.class);
		MultipleOutputs.addNamedOutput(job, "France", TextOutputFormat.class, CountryData.class, FloatWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setJarByClass(MultiOutputMapReduceTest.class);
		
		job.waitForCompletion(true);
		
	}
	
}
