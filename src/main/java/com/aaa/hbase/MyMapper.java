package com.aaa.hbase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MyMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private BufferedReader br = null;
	private Date date = null;
	
	// Htable instance
	private HTable table = null;
	
	/**
	 * Called once at the beginning of the task.
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		table = new HTable(context.getConfiguration(), HBaseInputTest.TABLE_NAME);  // create HTable instance
		table.setAutoFlush(false, true); // put instance buffer 자동 비우기 off, fail 일 경우 buffer clear
		
	}

	/**
	 * Called once for each key/value pair in the input split. Most applications
	 * should override this, but the default is the identity function.
	 * 
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
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
				
				// row key
				String rowKey = new String(columns[4]+","+columns[1]+","+columns[2]+","+columns[3]+","+columns[5]+","+columns[6]+","+columns[7]+","+date.toString());
				
				// put instance List
				List<Put> puts = new ArrayList<Put>();
				
				// rowKey에 대한 put instance
				Put put = new Put(Bytes.toBytes(rowKey));
				
				// reduce value : 해당 파일을 찾은 후 파일에서 데이터 값을 읽어온다.
				String pathStr = "/user/test/input/INTLCTRY/data/" + columns[0].trim(); // file path
				Path path = new Path(pathStr); 
				FileSystem fs = FileSystem.get(URI.create(pathStr), context.getConfiguration());
				
				// 파일 존재 여부 체크
				if (fs.isFile(path)) {
					
					br = new BufferedReader(new InputStreamReader(fs.open(path)));
					
					while (br.ready()) {
						
						String line = br.readLine();
						
						if (line.equals("DATE,VALUE")) continue;	// csv file header skip... (csv 이므로 구분자 (,) 사용)
						
						String[] valueColumns = line.toString().trim().split(",");
						
						if(valueColumns != null && valueColumns.length == 2 && !valueColumns[1].equals(".")) {
							
							String time = valueColumns[0].replaceAll("-", "");
							long ts = Long.parseLong(time);  // time stamp
							
							
							// 출발 지연을 Column Family에 time stamp와 같이 추가한다. qualifier는 data로 지정한다.
							put.add(HBaseInputTest.COLUMN_FAMILY, Bytes.toBytes("data"), ts, Bytes.toBytes(valueColumns[1]));
							puts.add(put);
							
						}
						
					}
					
					// List<Put> 객체를 HBase 테이블에 Put한다.
					table.put(puts);
					
				}
				else {
					
					System.out.println("File is not found.");
					
				}
				
			}
			
		}
		
	}

	/**
	 * Called once at the end of the task.
	 * Mapper가 종료할 때 스트림을 닫고 table을 flush한 다음 종료
	 */
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		br.close();
		table.flushCommits();
		table.close();
		
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
