package com.aaa.mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author 131084
 *
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	// Mapper output key variable
	private Text outputKey = new Text();
	// File Buffer Reader variable
	private BufferedReader br = null;
	// DateFormat instance variable
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	// Date instance variable
	private Date date = null;

	/**
	 * Called once at the end of the task.
	 * map 작업이 종료되면 BufferedReader stream을 닫는다.
	 */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		br.close();
	}
	
	/**
	 * Called once for each key/value pair in the input split. Most applications
	 * should override this, but the default is the identity function.
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// input value 중 파일명 확인
		if (value.toString().contains("\\") || value.toString().contains("csv")) {
			
			// Reduce Key 값으로 사용될 문자열 배열을 출력 (File Path, 경제 활동 인구 종류, 연령대, 성별, 국적, Units(단위), Frequency(빈도), Seasonal Adjustment (경제통계의 원계수에서 계절변동을 제거하는 것), Last Updated)
			String[] columns = extractCol(value.toString());
			
			if (columns != null) {
				
				try {
					date = sdf.parse(columns[8]);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				
				// reduce key : 국적+경제 활동 인구 종류+연령대+성별+Units(단위)+Frequency(빈도)+Seasonal Adjustment (경제통계의 원계수에서 계절변동을 제거하는 것)+Last Updated
				outputKey.set(columns[4]+","+columns[1]+","+columns[2]+","+columns[3]+","+columns[5]+","+columns[6]+","+columns[7]+","+date);
				
				// reduce value : 해당 파일을 찾은 후 파일에서 데이터 값을 읽어온다.
				String pathStr = "/user/input/testData/INTLCTRY/data/" + columns[0].trim(); // file path
				Path path = new Path(pathStr); 
				FileSystem fs = FileSystem.get(URI.create(pathStr), context.getConfiguration());
				
				// 파일 존재 여부 체크
				if (fs.isFile(path)) {
					
					br = new BufferedReader(new InputStreamReader(fs.open(path)));
					
					while (br.ready()) {
						
						String line = br.readLine();
						
						if (line.equals("DATE,VALUE")) continue;	// csv file header skip... (csv 이므로 구분자 (,) 사용)
						
						context.write(outputKey, new Text(line));   
						
					}
					
				}
				else {
					
					System.out.println("File is not found.");
					
				}
				
			}
			
		}
		
	}
	
	// input file(meta file)로부터 라인별 문자열 추출 / file context format => (File path; Title; Units; Frequency; Seasonal Adjustment; Last Updated)
	private String[] extractCol(String line) {
		
		String[] columns = new String[9];	// 가공 데이터 내역 배열 변수
		String[] tmpCol  = line.split(";"); // file context format => (File path; Title; Units; Frequency; Seasonal Adjustment; Last Updated)
		
		if (tmpCol != null && tmpCol.length == 6) {
			
			// Title value split
			String[] subColumns = tmpCol[1].split(":");
			
			if (subColumns != null && subColumns.length > 0) {
				
				// 경제 활동 인구 종
				if( subColumns[0].trim().equals("Active Population") || subColumns[0].trim().equals("Activity Rate") ||
					subColumns[0].trim().equals("Employed Population") || subColumns[0].trim().equals("Employment Rate") || 
					subColumns[0].trim().equals("Employment to Population Rate") || subColumns[0].trim().equals("Inactive Population") ||
					subColumns[0].trim().equals("Inactivity Rate") || subColumns[0].trim().equals("Unemployed Population") ||
					subColumns[0].trim().equals("Unemployment Rate") || subColumns[0].trim().equals("Unemployment to Population Rate") ||
					subColumns[0].trim().equals("Working Age Population")) {
					
					columns[0] = tmpCol[0].trim().replace('\\', '/');	// File Path
					columns[1] = subColumns[0].trim();	// 경제 활동 인구 종류
					columns[2] = subColumns[1].trim();	// 연령대
					columns[3] = subColumns[2].substring(0, subColumns[2].indexOf("for")).trim(); 	// 성별
					columns[4] = subColumns[2].substring(subColumns[2].indexOf("for")+3).trim();	// 국적
					columns[5] = tmpCol[2].trim(); // Units(단위)
					columns[6] = tmpCol[3].trim(); // Frequency(빈도)
					columns[7] = tmpCol[4].trim(); // Seasonal Adjustment (경제통계의 원계수에서 계절변동을 제거하는 것)
					columns[8] = tmpCol[5].trim(); // Last Updated
					
					return columns;
					
				}
			}
			
		}
		
		return null;
	}
	
}
