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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;

public class WordCount extends Configured implements Tool {

	//Map Class.
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		static enum Counters {
			
			INPUT_WORDS
		}

		private final static IntWritable one = new IntWritable(1);

		private Text word = new Text();

		private boolean caseSensitive = true;

		private Set<String> patternsToSkip = new HashSet<String>();

		private long numRecords = 0;

		private String inputFile;

		public void configure(JobConf job) {
			caseSensitive = job.getBoolean("wordcount.case.sensitive", true);
			inputFile = job.get("map.input.file");

			if (job.getBoolean("wordcount.skip.patterns", false)) {
				Path[] patternsFiles = new Path[0];
				try {
					patternsFiles = DistributedCache.getLocalCacheFiles(job);
				}
				catch (IOException ioe) {
					System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
				}
				for (Path patternsFile : patternsFiles) {
					parseSkipFile(patternsFile);
				}
			}
		}

		private void parseSkipFile(Path patternsFile) {
			try {
				BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
				String pattern = null;
				while ((pattern = fis.readLine()) != null) {
					patternsToSkip.add(pattern);
				}
			}
			catch (IOException ioe) {
				System.err.println("Caught exception while parsing the cached file '" + patternsFile + "' : "
						+ StringUtils.stringifyException(ioe));
			}
		}
		
		// TextInputFormat => key : LongWritable 형식으로 라인번호, value : Text 형식으로 라인의 문자열
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
			
			for (String pattern : patternsToSkip) {
				line = line.replaceAll(pattern, "");
			}

			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
				reporter.incrCounter(Counters.INPUT_WORDS, 1);
			}

			if ((++numRecords % 100) == 0) {
				reporter.setStatus("Finished processing " + numRecords + " records " + "from the input file: " + inputFile);
			}
		}
	}

	
	//Reduce Class.
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		
		// TextOutputFormat => 출력값을 일반 텍스트로 출력할 때 지정. key, value 사이는 TAB으로 구분.
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	//Run method.
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), WordCount.class);
		conf.setJobName(WordCount.class.getName());

		conf.setJarByClass(WordCount.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		// conf.setp
		/*“.jar” 파일에 경로를 입력 한다. 여기서 말하는 경로는 “HDFS“의 경로가 아니라 클라이언트의 로컬 경로를 말한다.
		예를 들어서 “c:/mr.jar”에 라이브러리가 있을 경우 conf.setjar("c:/mr.jar")으로 선언 한다.
		상당히 중요한 부분이며, 여기에 설정된 “.jar“가 “JobClient“를 통해서 “HDFS“에 저장되고,
		“Job Tracker“에게 저장된 경로 정보를 전달 한다.*/  
//		conf.setJar(args[0]);
		
		FileInputFormat.setInputPaths(conf, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		// “JobConf” 오브젝트에서 최종 설정 및 옵션 정보를 취합해서 “Name Node“, “Job Tracker“로 “.jar” 파일 과 함께 전송 한다.
		JobClient.runJob(conf);
		return 0;
	}
}