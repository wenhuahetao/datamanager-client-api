package com.fline.hadoop.examples;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import junit.framework.TestCase;
import com.fline.hadoop.hbase.HbaseOperator;

public class HbaseOperatorTest extends TestCase {

//	final static String URL = "http://114.215.249.44:8084/fline_hdfs";  
//	final static String URL = "http://fdp-master:8087/fline_hdfs";  
	final static String URL = "http://localhost:8080/fline_hdfs"; 
 
//	@Test
//	public void testHbaseImportDetail() { 
//		
//		String dbType = "mysql";
//		String connectionUrl = "121.40.19.144";
//		String username = "zhangyue";
//		String password = "123456";
//		String dbName = "lvbb";
//		String rdbcolumns = "id,serial_number,batch_number,occur_date,biz_type,income,pay,description,state";
//		String linenum = "79";
//		String tableName = "";
//		String rowkeyparam = "timestamp,0,radom16";
//		String partitioncolumn = "id";
//		String rdbtablename = "lvbb_balance";
//		String familys = "n";
//		String hbasecolumns = "n:id,n:serial_number,n:batch_number,n:occur_date,n:biz_type,n:income,n:pay,n:description,n:state";
//		String port = "3306";
//		HbaseOperator.hbaseImportDetail(dbType, connectionUrl, username, password, dbName, rdbcolumns, linenum,
//				tableName, rowkeyparam, partitioncolumn, rdbtablename, familys,hbasecolumns, port, URL);
//	}
	
	@Test
	public void testSearchProgressDetail() { 
		String progress = HbaseOperator.searchProgressDetail("HBASE_1446787655207", URL);
		System.out.println("testProgress:"+progress);
	}
//	
	@Test
	public void testImportRdbTable2Hbase() { 
//		 String dbType = "oracle";
//		 String dbName = "orcl";
//		 String connectionUrl = "localhost";
//		 String userName = "hetaotest";
//		 String password = "hetaotest";
//		 String tableName = "test1";
		
		String dbType = "mysql";
		String dbName = "lvbb";
		String connectionUrl = "121.40.19.144";
		String userName = "zhangyue"; 
		String password = "123456";
		String tableName = "lvbb_vehicle";
		String result = HbaseOperator.importRdbTable2Hbase(dbType, dbName,
				connectionUrl, userName, password, tableName, URL);
		
	}
	
	@Test
	public void testImportMultiTable2Hhbase() { 
		String dbType = "mysql";
		String dbName = "flinebigdata";
		String connectionUrl = "121.40.19.144";
		String userName = "zhangyue"; 
		String password = "123456";
		String[] RDBcolumns = {"main.id as id","main.name as name","main.content as content",  
				"sub.id as sub_id","sub.title as sub_title","sub.content as sub_content","sub.main_id as main_id"};
		String[] tables = {"main","sub"};
		String tableJoinCondition = " main,sub where main.id=sub.main_id";
		try {
			String result = HbaseOperator.importMultiTable2Hhbase(dbType, dbName,
					connectionUrl, userName, password, RDBcolumns, tables,tableJoinCondition,URL);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testSearchProgress() { 
		String progress = HbaseOperator.searchProgress("HBASE_1446787655207", URL);
		System.out.println("testProgress:"+progress);
	}
//
//	@Test
//	public void testUpdate() { 
//		try {
//			String tablename = "hb_lvbb_vehicle";
//			String rowkey = "111111";
//			String colnames = "sim:fm";
//			String line = "5676";
//			HbaseOperator.update(tablename, colnames, line, rowkey, URL);
//			System.out.println("testUpdate:ok");
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//
//	@Test
//	public void testSearchContent() {
//		String tableName = "lvbb_vehicle";
//		byte[] rowkey = "111111".getBytes();
//		System.out.println(rowkey);
//		String json = HbaseOperator.searchContent4json(tableName, rowkey, URL);
//		System.out.println("testSearchContent:" + json);
//	}
//
//	@Test
//	public void testSearchContents() {
//		try {
//			String tableName = "lvbb_vehicle";
//			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:ms");
//			long startRowkey = sdf.parse("2015-11-05 00:00:00").getTime();
//			long endRowkey = sdf.parse("2015-11-05 23:59:59").getTime();
//			String json =  HbaseOperator.searchContents4json(tableName, startRowkey,endRowkey, URL);
//			if(json.length()>=130){
//				System.out.println("testSearchContents:"+json.substring(0,130));
//			}else{
//				System.out.println("testSearchContents:"+json);
//			}
//		} catch (ParseException e) {
//			e.printStackTrace();
//		}
//	}
//
	@Test
	public void testSearchData() {
		try {
			String tableName = "C_ENT_BASICINFO";
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:ms");
			long startRowkey = sdf.parse("2015-11-06 00:00:00").getTime();
			long endRowkey = sdf.parse("2015-11-06 23:59:59").getTime();
			String json =  HbaseOperator.searchData4json(tableName, startRowkey,endRowkey,URL);
			if(json.length()>=130){
				System.out.println("testSearchData:"+json.substring(0,130));
			}else{
				System.out.println("testSearchData:"+json);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
//
//	@Test
//	public void testDelete() {
//		try {
//			String tablename = "hb_lvbb_vehicle";
//			byte[] rowkey = "322323".getBytes();
//			System.out.println(rowkey);
//			HbaseOperator.delete(tablename, rowkey, URL);
//			System.out.println("testDelete:ok");
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//	

		
}
