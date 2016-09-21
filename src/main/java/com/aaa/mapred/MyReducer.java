package com.aaa.mapred;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author 131084
 *
 */
public class MyReducer extends Reducer<Text, Text, Text, FloatWritable> {

	/**
	 * This method is called once for each key. Most applications will define
	 * their reduce class by overriding this method. The default implementation
	 * is an identity function.
	 * 
	 * reduce를 호출하기 전에 key별 partitioning이 됨.
	 * 따라서 해당 key에 속한 value들의 iterator 값들을 어떤식으로 가공하여 출력할 것인지를 처리해 주면 됨.
	 */
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		float sum = 0;
		int count = 0;
		
		// reduce key 별 value iterator loop 처리
		for(Text value: values) {
			
			String[] columns = value.toString().trim().split(",");	// reduce value
			
			if (columns != null && columns.length == 2 && !columns[1].equals(".")) {
				
				count++;
				sum += Float.parseFloat(columns[1]);
				
			}
			
	    }
		
		// 평균값 출력
		context.write(key, new FloatWritable(sum / count));
	}
	
}
