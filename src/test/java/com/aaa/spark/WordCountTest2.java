package com.aaa.spark;


import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.junit.Test;

import scala.collection.immutable.List;

public class WordCountTest2 implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3041450771755256390L;

	@Test
	public void testWordCount () throws Exception {
		
		String inputFile = "/user/scom/README.md";
	    String outputFile = "/user/scom/word_count";
	    
	    // Create a Java Spark Context.
	    SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("spark://master:7077");
	    
	    conf.setJars(new String[]{"target/mapred-0.0.1-SNAPSHOT.jar"});
//	    conf.setSparkHome("/user/vagrant/spark-1.0.2-bin-hadoop1");
	    
//	    conf.set("SPARK_LOCAL_IP", "localhost");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    // Load our input data.
	    JavaRDD<String> input = sc.textFile(inputFile);
	   /* // Split up into words.
	    JavaRDD<String> words = input.flatMap(
	  	      new FlatMapFunction<String, String>() {
	  	        public Iterable<String> call(String x) {
	  	          return Arrays.asList(x.split(" "));
	  	        }
	  	      }
	  	);
	    
  	    // Transform into word and count.
  	    JavaPairRDD<String, Integer> counts = words.mapToPair(
  	      new PairFunction<String, String, Integer>(){
  	        public Tuple2<String, Integer> call(String x){
  	          return new Tuple2(x, 1);
  	        }}).reduceByKey(new Function2<Integer, Integer, Integer>(){
  	            public Integer call(Integer x, Integer y){ return x + y;}});
  	    // Save the word count back out to a text file, causing evaluation.
  	    counts.saveAsTextFile(outputFile);*/
	    
	    input.count();
		System.out.println(input.count());
	}
	
}
