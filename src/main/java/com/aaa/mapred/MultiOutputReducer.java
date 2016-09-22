package com.aaa.mapred;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.aaa.datatype.CountryData;
import com.client.hdfs.common.Country;

public class MultiOutputReducer extends Reducer<CountryData, Text, CountryData, FloatWritable> {
	
	MultipleOutputs<CountryData, FloatWritable> mos = null;
	
	
	/**
	 * Called once at the start of the task.
	 */
	protected void setup(Context context) throws IOException, InterruptedException {

		mos = new MultipleOutputs<CountryData, FloatWritable>(context);
		
	}
	
	/**
	 * Called once at the end of the task.
	 */
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		mos.close();
		
	}
	
	@Override
	public void reduce(CountryData key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		float sum = 0;
		int count = 0;
       
		for (Text value : values) {
			String[] columns = value.toString().trim().split(",");
			
			if(columns != null && columns.length == 2 && !columns[1].equals(".")) {
				count++;
				sum += Float.parseFloat(columns[1]);
			}
		}
		
		Country country = Country.getCountry(key.getCountry());
		
		if (country != null) {
		
			System.out.println(country.countryName());
		
			mos.write(country.countryName(), key, new FloatWritable(sum/count));
		
		}
		else {
		
			System.out.println("Country is not found");
			
		}
		
	}

}
