package com.aaa.spark;


import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCountTest implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3041450771755256390L;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		
//		String inputFile = "hdfs://master:9000/user/scom/README.md";
//		String outputFile = "hdfs://master:9000/user/scom/word_count";
		String inputFile = "D:/HDFS/spark-1.3.0-bin-hadoop2.3/README.md";
		String outputFile = "D:/tmp/test/word_count";
	    
	    // Create a Java Spark Context.
//	    SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local");
	    SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("spark://master:7077");	    
//	    SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("yarn-client");
	    
//	    conf.setJars(new String[]{"target/mapred-0.0.1-SNAPSHOT.jar"});
//	    conf.set("spark.driver.host","10.149.181.149");
//	    conf.setSparkHome("/user/vagrant/spark-1.0.2-bin-hadoop1");
	    
//	    conf.set("SPARK_LOCAL_IP", "localhost");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    // Load our input data.
	    JavaRDD<String> input = sc.textFile(inputFile);
	    
//	    System.out.println(input.first());
	    
	    // Split up into words.
	    JavaRDD<String> words = input.flatMap(
	  	      new FlatMapFunction<String, String>() {
	  	        public Iterable<String> call(String x) {
	  	          return Arrays.asList(x.split(" "));
	  	        }
	  	      }
	  	);
	    
	    
//	    System.out.println(input.collect());
	    
	        
  	    // Transform into word and count.  => words RDD(flatMap 형식(나열되어 있음))를  map 형식(Map(String, Int))의 입력 포맷으로 변환 후 reduceByKey 호출
	    // reduceByKey에서 key 부분은 보이지 않는다. 조금 생각해 보면 어짜피 key 별로 grouping을 한 다음, key가 아닌 value 부분에 대해 reduce를 적용할 것으므로, 굳이 key 부분이 보이지 않아도 상관없다는 생각이 들 것이다.
  	    JavaPairRDD<String, Integer> counts = words.map(
  	      new PairFunction<String, String, Integer>(){
  	        public Tuple2<String, Integer> call(String x){
  	          return new Tuple2(x, 1);
  	        }}).reduceByKey(new Function2<Integer, Integer, Integer>(){
  	            public Integer call(Integer x, Integer y){ return x + y;}});
  	    // Save the word count back out to a text file, causing evaluation.
  	    
  	    
  	    
  	    counts.saveAsTextFile(outputFile);
	    
//	    input.count();
//		System.out.println(input.count());
	}
	
}
