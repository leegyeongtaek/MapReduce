package com.aaa.hdfs;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.client.hdfs.common.RemoteConfig;

/**
 * Read files from Remote HDFS
 * @author 131084
 *
 */
public class FileTest {
	
	static final String srcDir = "/user/input/testData/";
	
	@Test
	public void testReadFile () throws Exception {
		
		String fileName = "wordcount.txt";
		
		Configuration conf = RemoteConfig.getConf();
		
		Path path = new Path(srcDir + fileName);
		
		FileSystem fs = FileSystem.get(URI.create(srcDir + fileName), conf);
		
		if (fs.exists(path)) {
			
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			
			while (br.ready()) {
				
				String line = br.readLine();
				System.out.println(line);
				
			}
			
			FileStatus fStatus = fs.getFileStatus(path);
			
			if (fStatus.isFile()) {
				
				System.out.println("");
				System.out.println("=================================");
				System.out.println("File Block Size : " + fStatus.getBlockSize());
				System.out.println("Group of File   : " + fStatus.getGroup());
				System.out.println("Owner of File   : " + fStatus.getOwner());
				System.out.println("File Length     : " + fStatus.getLen());
				System.out.println("=================================");
				
			} 
			else {
				System.out.println("This is not File.");
			}
			
		}
		else {
			System.out.println("File is not found.");
		}
		
	}
	

}
