package com.aaa.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

/**
 * 
 * @author 131084
 * HBase Table에서 row key 단위로 데이터를 읽어들여 이중 qualifier가 data인 데이터를 reduce에 write 함.
 * 
 */
public class HBaseMapper extends TableMapper<Text, FloatWritable> {

	private Text outKey = new Text();
	private FloatWritable outValue = new FloatWritable();
	
	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
		
		outKey.set(new String(value.getRow()));  // reduce key : Key Row return
		
		// row의 Cell 배열 객체를 반환 후 loop 처리
		for (Cell c : value.rawCells()) {
			
			String qualifier = new String(CellUtil.cloneQualifier(c));	// column family의 qualifier
			
			if (qualifier.equals("data")) {
				
				Float f = Float.parseFloat(new String(CellUtil.cloneValue(c)));
				
				outValue.set(f);
				
				context.write(outKey, outValue);
				
			}
			
		}
		
	}
	
}