package com.aaa.hbase;

import org.junit.Test;
import org.mortbay.log.Log;

import com.client.hdfs.common.HbaseRemoteConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Client remote connect HBase & create table
 * @author 131084
 * 
 * 
 * Cell Unit => "rowkey" : {
 * 					"cf1" : {
 * 						"qualifier1" : {
 * 							1475643305551 : "value1" 
 * 						}
 * 					},
 * 					"cf2" : {
 * 						"qualifier2" : {
 * 							1475643305572 : "value2" 
 * 						}
 * 					},
 * 					"cf3" : {
 * 						"qualifier3" : {
 * 							1475643305582 : "value3" 
 * 						}
 * 					},
 * 				}
 */
public class HBaseAPISampleTest {
	
	@Test
	public void testHBaseAPISample () throws Exception {
		
		// {rowkey, column family, qualifier, time stamp/version} => value
		String tableName 	= "test2";	// table name
		String rowKey 		= "rowkey";	// row key (Primary Key)
		String[] cfs 	= new String[] { "cf1", "cf2", "cf3" };	// column family (column의 집합)				
		String[] qfs 	= new String[] { "qualifier1", "qualifier2", "qualifier3" }; // qualifier (column)
		String[] vals 	= new String[] { "value1", "value2", "value3" };  // value	

		// configuration
		Configuration config = HbaseRemoteConfig.getConf(null);
        
		// 테이블 생성 & 컬럼 추가
		HBaseAdmin hbase = null;
		
		try {
			
			hbase = new HBaseAdmin(config);  // HBase 의 table 메타 데이터를 관리. 일반적인 운용함수를 포함하는 인터페이스를 제공하는 클래스. (table 생성, 삭제, 활성화, 비활성화 기능 담당)
			
			if (hbase.tableExists(tableName)) {
				
				Log.debug("Table is exist!");
				
				// HTable 은 개개의 table로 부터 데이터를 추가, 업데이트, 삭제하는 작업 수행.
				HTable table = new HTable(config, tableName);
				
				// 행 삭제
				List<Delete> list = new ArrayList<Delete>();
				Delete d1 = new Delete(rowKey.getBytes()); // 단일 row에 대한 Delete 작업을 수행할 때 사용. rowkey에 해당하는 Cell을 삭제.
				list.add(d1);
				table.delete(list); // Delete 리스트 내의 모든 Cell을 bulk로 삭제.
		
				// 테이블 drop
				hbase.disableTable(tableName);
				hbase.deleteTable(tableName);
				
			}
			else {
			
				HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));  // HBase의 Table에 대한 상세 정보를 포함하는 클래스. Table에 대한 속성 값을 관리.
				
				// HBase column family 관리
				for (int i = 0; i < cfs.length; i++) {
					HColumnDescriptor meta = new HColumnDescriptor(cfs[i].getBytes());  // column family instance 생성.
					desc.addFamily(meta);  // column family 추가
				}
				
				hbase.createTable(desc); // create table (create 'table2', 'colfamily1', 'colfamily2', 'colfamily3')
				
				// HTable 은 개개의 table로 부터 데이터를 추가, 업데이트, 삭제하는 작업 수행.
				HTable table = new HTable(config, tableName);
				
				try {
					
					// 행 추가 (put) => rowkey, column family, qualifier, value
					for (int i = 0; i < cfs.length; i++) {
						
						// 단일 Key Row에 대한 Put 작업을 수행함.
						Put put = new Put(Bytes.toBytes(rowKey));
						put.add(Bytes.toBytes(cfs[i]), Bytes.toBytes(qfs[i]), Bytes.toBytes(vals[i]));
						table.put(put);
						
					}
			
					// 테이블 데이터 조회 
					Scan s = new Scan();  // (Scan 작업 수행. 시작 Row를 지정하지 않으면 모든 Row를 scan함.)
					ResultScanner rs = table.getScanner(s);  // HBase의 클라이언트 측 Scanning을 위한 인터페이스
					
					// ResultScanner에 대한 단일 Key Row에 대한 결과 객체
					for (Result r : rs) {
						
						// Cell 반환 (Cell 인터페이스는 다음과 같은 필드로 구성되는 HBase의 저장 단위 임(7가지) : row, column family, column qualifier, timestamp, type, version, value)
						for (Cell c : r.rawCells()) {
						 
							System.out.println("row :" + new String(CellUtil.cloneRow(c)) + "");
							System.out.println("family :" + new String(CellUtil.cloneFamily(c)) + ":");
							System.out.println("qualifier :" + new String(CellUtil.cloneQualifier(c)) + "");
							System.out.println("value :" + new String(CellUtil.cloneValue(c)));
							System.out.println("timestamp :" + c.getTimestamp() + "");
							System.out.println("-------------------------------------------");
						 
						}
					}
			

				} catch (IOException e) {
					e.printStackTrace();
				}
			
			}
		}
		finally {
			if (hbase != null)
				hbase.close();
		}

	}

}
