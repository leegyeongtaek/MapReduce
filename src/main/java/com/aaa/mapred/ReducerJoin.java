package com.aaa.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.aaa.datatype.CountryData;

public class ReducerJoin extends Reducer<Text, Text, CountryData, Text> {
	
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
		
		CountryData outputKey = new CountryData();
		Text outputValue = new Text();
		
		// reduce key 별 value iterator loop 처리
		for(Text value: values) {
			
			String[] columns = value.toString().trim().split(",");	// reduce value
			
			if (columns != null && columns.length == 9) {
				
				outputKey.setCountry(columns[0]);
				outputKey.setTitle(columns[1]);
				outputKey.setAge(columns[2]);
				outputKey.setSex(columns[3]);
				outputKey.setUnits(columns[4]);
				outputKey.setFrequency(columns[5].charAt(0));
				outputKey.setSeasonalAdjust(columns[6]);
				outputKey.setUpdateDate(columns[7]);
				
				outputValue.set(","+columns[8]+","+columns[9]);
				
				context.write(outputKey, outputValue);
				
			}
			
	    }
		
	}
	
}
