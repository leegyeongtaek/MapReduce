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
package pe.beyondj2ee.hadoop.mr.test;

import org.junit.Test;

import pe.beyondj2ee.hadoop.mr.MRLauncher;
import pe.beyondj2ee.hadoop.mr.WordCount;

public class MRLauncherTest {

	@Test
	public void testWordCount () throws Exception {
		
		ClassLoader cl = MRLauncher.class.getClassLoader();
		String jarAbsolutePath = cl.getResource("mapred-0.0.1-SNAPSHOT.jar").toString();
		
		MRLauncher mrl = new MRLauncher();
		String [] args = new String[3];
		
		args[0] = jarAbsolutePath;		// 배포할 ".jar"의 물리적 경로 -> 추후 빌드/배포 고려해서 mapred-0.0.1-SNAPSHOT.jar를 "classpath"에 저장.
		args[1] = "/user/test/wordcount.txt";	// 입력 디렉토리 정보를 설정 한다. (“HDFS”의 경로)
		args[2] = "/user/output/wordcount";	// MR이 수행 완료후 결과 디렉토리를 설정 한다.
		
		mrl.invokeMR(WordCount.class.getName(), args);	//  “MRLauncher” 오브젝트를 수행
		
	}
	
}