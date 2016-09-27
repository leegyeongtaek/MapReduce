package com.aaa.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;

import com.aaa.datatype.CountryData;
import com.client.hdfs.common.RemoteConfig;

/**
 * 
 * @author 131084
 * 경제 활동 인구 meta 정보를 가지고 해당 csv 파일을 찾은 후 평균 data 출력
 */
public class SortMapReduceTest {
	
	static final String inputPath = "/user/test/input/INTLCTRY/README_TITLE_SORT.txt";
	static final String outputPath = "/user/output/INTLCTRY/sortOutput_INTLCTRY1";

	@Test
	public void testSortMapReduce () throws Exception {
		
		Configuration conf = RemoteConfig.getConf();
		
		// 설정 내용을 포함한 job instance 생성
		Job job = Job.getInstance(conf, "Extract Columns");
		
		// job output key, value 만 데이터 타입 클래스을 선언하는 이유는 input 인 경우 InputFormatClass를 선언할때 내부에서 데이터 타입 클래스가 결정되기 때문이다.
		job.setOutputKeyClass(CountryData.class);
		job.setOutputValueClass(Text.class);
		
		// Mapper class, Reducer class 선언
		job.setMapperClass(SortMyMapper.class);
		job.setReducerClass(SortMyReducer.class);
		
		// HDFS에 입출력할 format class 선언
		job.setInputFormatClass(TextInputFormat.class);  	// TextInputformat 일 경우 key는 라인번호, value는 문자열이 된다.
		job.setOutputFormatClass(TextOutputFormat.class);	// TextOutputforamt 일 경우 key, value는 TAB으로 구분 된다.
		
		// 입출력 path 설정
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setJarByClass(SortMapReduceTest.class);
		
		job.waitForCompletion(true);
		
	}
	
}
