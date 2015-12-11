package com.fline.hadoop.hbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import com.fline.hadoop.httpClient.HttpClientUtil;
import com.fline.hadoop.util.EncryptionDecryption;

/**
 * HBase operation class
 * @author hetao 
 *
 */
public class HbaseOperator {

	final static String HBASE_URL_UPDATE = "/hbase/update";
	final static String HBASE_URL_DELETE = "/hbase/delete";
	final static String HBASE_URL_IMPORT = "/collection/importRdbTable2Hbase";
	final static String HBASE_URL_IMPORT_MULTI_TABLE = "/collection/importMultiTable2Hbase";
	final static String HBASE_URL_IMPORT_DETAIL = "/hbase/hbaseImportDetail";
	final static String HBASE_URL_PROGRESS = "/collection/searchProgress";
	final static String HBASE_URL_PROGRESS_DETAIL = "/hbase/searchProgress";
	final static String HBASE_URL_SEARCH_DATA = "/hbase/searchData";
	final static String HBASE_URL_SEARCH_CONTENTS = "/hbase/searchContents";
	final static String HBASE_URL_SEARCH_CONTENT = "/hbase/searchContent";
	
	/**
	 * Query, enter the range of rowkey and table name, function to return to the range of all rowkey
	 * @param tableName:  Relational database table name
	 * @param startRowkey:  Start time
	 * @param endRowkey:  End time
	 * @param url:  Big data platform address
	 * @return
	 */
	public static List<byte[]> searchData(String tableName,long startRowkey,long endRowkey,String url){
		String content = searchData4json(tableName,startRowkey,endRowkey,url);
		JSONObject json = JSONObject.fromObject(content);
		List<byte[]> result = (List<byte[]>) json.get("contents");
		return result;
	}
	
	/**
	 * Query, enter the range of rowkey and table name, function to return to the range of all rowkey , Generate JSON
	 * @param tableName: Relational database table name
	 * @param startRowkey: Start time
	 * @param endRowkey: End time
	 * @param url: Big data platform address
	 * @return
	 */
	public static String searchData4json(String tableName,long startRowkey,long endRowkey,String url){
		Map<String,String> paramMap = new HashMap<String,String>();
		paramMap.put("tablename",tableName);
		paramMap.put("startRowkey",startRowkey+"");
		paramMap.put("endRowkey",endRowkey+"");
		System.out.println(url + HBASE_URL_SEARCH_DATA);
		return HttpClientUtil.getHttp(url + HBASE_URL_SEARCH_DATA, paramMap);
	}
	
	/**
	 * Query specifying the rowkey corresponding to the data
	 * @param tableName: Relational database table name
	 * @param rowkey:  Your appointed time
	 * @param url: Big data platform address
	 * @return
	 */
	public static List<Object> searchContent(String tableName,byte[] rowkey,String url){
		String content = searchContent4json(tableName,rowkey,url);
		JSONObject json = JSONObject.fromObject(content);
		List<Object> result = (List<Object>) json.get("contents");
		return result;
	}
	
	/**
	 * Query specifying the rowkey corresponding to the data ,Generate JSON
	 * @param tableName: Relational database table name
	 * @param rowkey: Your appointed time
	 * @param url: Big data platform address
	 * @return
	 */
	public static String searchContent4json(String tableName,byte[] rowkey,String url){
		try {
			Map<String,String> paramMap = new HashMap<String,String>();
			paramMap.put("tablename",tableName);
			EncryptionDecryption des = new EncryptionDecryption("wf");
			paramMap.put("rowkey",des.byteArr2HexStr(rowkey));
			return HttpClientUtil.getHttp(url + HBASE_URL_SEARCH_CONTENT, paramMap);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";
	}
	
	
	/**
	 * Query rowkey range of all data
	 * @param tableName: Relational database table name
	 * @param startRowkey: Start time
	 * @param endRowkey: End time
	 * @param url: Big data platform address
	 * @return
	 */
	public static List<Object[]> searchContents(String tableName,long startRowkey,long endRowkey,String url){
		String content = searchContents4json(tableName,startRowkey,endRowkey,url);
		JSONObject json = JSONObject.fromObject(content);
		List<Object[]> result = (List<Object[]>) json.get("contents");
		return result;
	}
	
	/**
	 * Query rowkey range of all data,Generate JSON
	 * @param tableName: Relational database table name
	 * @param startRowkey: Start time
	 * @param endRowkey: End time
	 * @param url: Big data platform address
	 * @return
	 */
	public static String searchContents4json(String tableName,long startRowkey,long endRowkey,String url){
		Map<String,String> paramMap = new HashMap<String,String>();
		paramMap.put("tablename",tableName);
		paramMap.put("startRowkey",startRowkey+"");
		paramMap.put("endRowkey",endRowkey+"");
		return HttpClientUtil.getHttp(url + HBASE_URL_SEARCH_CONTENTS, paramMap);
	}
	
	/**
	 * According to the data source and tableName, import table data to HBase
	 * @param dbType:  Relational database type
	 * @param dbName: Relational database name
	 * @param connectionUrl: Relational database IP
	 * @param userName:  Relational database user name
	 * @param password:  Relational database password
	 * @param tableName: Relational database table name
	 * @param url: Big data platform address
	 * @return Hbase table name and HBase import only ID combination
	 */
	public static String importRdbTable2Hbase(String dbType,String dbName,
			String connectionUrl,String userName,String password,String tableName,String url){
		String mrOnekey = "HBASE_"+System.currentTimeMillis();
		Map<String,String> paramMap = new HashMap<String,String>();
		paramMap.put("dbtype",dbType);
		paramMap.put("dbname",dbName);
		paramMap.put("connectionurl",connectionUrl);
		paramMap.put("username",userName);
		paramMap.put("password",password);
		paramMap.put("tablename",tableName);
		paramMap.put("mrOnekey",mrOnekey);
		HttpClientUtil.postHttp(url + HBASE_URL_IMPORT, paramMap);
		return tableName+","+mrOnekey; 
	}
	
	/**
	 * @param dbType
	 * @param dbName
	 * @param connectionUrl
	 * @param userName
	 * @param password
	 * @param RDBcolumns 数据库字段集合
	 * @param tables 表名集合
	 * @param tableJoinCondition 表关系
	 * @param url
	 * @return
	 * @throws Exception
	 */
	public static String importMultiTable2Hhbase(String dbType, String dbName,
			String connectionUrl, String userName, String password,
			String[] RDBcolumns, String[] tables, String tableJoinCondition,
			String url) throws Exception {
		String mrOnekey = "HBASE_" + System.currentTimeMillis();
		Map<String, String> paramMap = new HashMap<String, String>();
		paramMap.put("dbtype", dbType);
		paramMap.put("dbname", dbName);
		paramMap.put("connectionurl", connectionUrl);
		paramMap.put("username", userName);
		paramMap.put("password", password);
		StringBuilder tableNameBuilder = new StringBuilder();
		tableNameBuilder.append("(SELECT ");
		StringBuilder realColumns = new StringBuilder();
		Map<String,String> familyMap = new HashMap<String,String>();
		
		for (int i = 0; i < RDBcolumns.length; i++) {
			String[] splits = RDBcolumns[i].split("\\s+as\\s+");  
			
			// only support "col as newcolname" or "col"
			if (splits.length == 2) {
				String realColumn = splits[1].trim();
				realColumns.append(realColumn);
				String tf = splits[0];
				String tn = tf.substring(0,tf.indexOf("."));
				String tnf = familyMap.get(tn);
				if(tnf==null){
					familyMap.put(tn, tn + ":" +realColumn);
				}else{
					tnf = tnf + "," + tn + ":"+ realColumn ;
					familyMap.put(tn, tnf);
				}
			} else if (splits.length == 1) {
				realColumns.append(splits[0].trim());
			} else {
				throw new Exception("Unsupport colname = " + RDBcolumns[i]);
			}
			
			tableNameBuilder.append(RDBcolumns[i]);
			if(i<RDBcolumns.length-1){
				realColumns.append(',');
				tableNameBuilder.append(',');
			}
		}
		tableNameBuilder.append(" from "); 
		String familys = "";
		String fmField = "";
		for (String key : familyMap.keySet()) {
			familys +=  "," + key;
			fmField += "," + familyMap.get(key);
		}
		tableNameBuilder.append(tableJoinCondition);
		tableNameBuilder.append(") as tmpTable");
		paramMap.put("fileds", realColumns.toString().trim());
		paramMap.put("tableName", tableNameBuilder.toString());
		paramMap.put("mrOnekey", mrOnekey);
		paramMap.put("familys", familys.substring(1));
		paramMap.put("fmField", fmField.substring(1));
		HttpClientUtil.postHttp(url + HBASE_URL_IMPORT_MULTI_TABLE, paramMap);
		return paramMap.get("tablename") + "," + mrOnekey;
	}

	
	/**
	 * Import data from famliy to HBase
	 * @param dbType:Relational DataBase Type
	 * @param connectionUrl:Relational DataBase IP
	 * @param userName:  Relational database user name
	 * @param password:  Relational database password
	 * @param dbName:Relational database name
	 * @param rdbcolumns:Relational database tableName Fileds
	 * @param linenum:Relational database table datas lines
	 * @param tableName:Relational database tableName
	 * @param rowkeyparam:rowkey
	 * @param partitioncolumn:Primary key
	 * @param rdbtablename:Relational database tableName
	 * @param familyParms:family Object
	 * @param port:DataBase port 
	 * @param url: Big data platform address
	 * @return
	 */
	public static String hbaseImportDetail(String dbType,String connectionUrl,String username,String password,String dbName,
			String rdbcolumns,String linenum,String tableName,String rowkeyparam,
			String partitioncolumn,String rdbtablename,String familys,String hbasecolumns,String port,String url){
		String mrOnekey = "HBASE_"+System.currentTimeMillis();
		Map<String,String> paramMap = new HashMap<String,String>();
		paramMap.put("dbtype",dbType);
		paramMap.put("dbName",dbName);
		paramMap.put("connectionurl",connectionUrl);
		paramMap.put("username",username);
		paramMap.put("password",password);
		paramMap.put("rdbcolumns",rdbcolumns);
		paramMap.put("mrOnekey",mrOnekey);
		paramMap.put("linenum",linenum);
		paramMap.put("hbasetable",tableName);
		paramMap.put("rowkeyparam",rowkeyparam);
		paramMap.put("partitioncolumn",partitioncolumn);
		paramMap.put("rdbtablename",rdbtablename);
		paramMap.put("familyParms","");
		paramMap.put("familys",familys);
		paramMap.put("hbasecolumns",hbasecolumns);
		paramMap.put("port",port);
		HttpClientUtil.postHttp(url + HBASE_URL_IMPORT_DETAIL, paramMap);
		return tableName+","+mrOnekey; 
	}
	
	/**
	 * Monitor data import
	 * @param mrOnekey:HBase import only ID, generated by calling importRdbTable2Hbase
	 * @param url: Big data platform address
	 * @return HBase import Progress And Time
	 */
	public static String searchProgress(String mrOnekey,String url){
		Map<String,String> paramMap = new HashMap<String,String>();
		paramMap.put("mrOnekey",mrOnekey);
		String content = HttpClientUtil.getHttp(url + HBASE_URL_PROGRESS, paramMap);
		System.out.println("content:"+content);
		if(StringUtils.isNotEmpty(content)){
			JSONObject json = JSONObject.fromObject(content);
			return json.get("contents").toString();
		}
		return null;
	}
	
	/**
	 * Monitor data import
	 * @param mrOnekey:HBase import only ID, generated by calling importRdbTable2Hbase
	 * @param url: Big data platform address
	 * @return HBase import Progress And Time
	 */
	public static String searchProgressDetail(String mrOnekey,String url){
		Map<String,String> paramMap = new HashMap<String,String>();
		paramMap.put("mrOnekey",mrOnekey);
		String content = HttpClientUtil.getHttp(url + HBASE_URL_PROGRESS_DETAIL, paramMap);
		System.out.println("content:"+content);
		if(StringUtils.isNotEmpty(content)){
			JSONObject json = JSONObject.fromObject(content);
			return json.get("contents").toString();
		}
		return null;
	}
	
	/**
	 * update data from HBase
	 * @param tableName: Relational database table name
	 * @param colnames: Column and famlily combination
	 * @param line: Column value
	 * @param rowkey: Your appointed hbase rowkey
	 * @param url: Big data platform address
	 * @throws Exception
	 */
	public static void update(String tableName, String colnames, String line,String rowkey,String url) throws Exception{
		Map<String,String> paramMap = new HashMap<String,String>();
		paramMap.put("tablename",tableName);
		paramMap.put("colnames",colnames);
		paramMap.put("rowkey",rowkey);
		paramMap.put("line",line);
		HttpClientUtil.postHttp(url + HBASE_URL_UPDATE, paramMap);
	}
	
	/**
	 * Delete data from HBase
	 * @param tableName: Relational database table name
	 * @param rowkey: Your appointed hbase rowkey
	 * @param url: Big data platform address
	 * @throws Exception
	 */
	public static void delete(String tableName, byte[] rowkey,String url) throws Exception{
		Map<String,String> paramMap = new HashMap<String,String>();
		paramMap.put("tablename",tableName);
		EncryptionDecryption des = new EncryptionDecryption("wf");
		paramMap.put("rowkey",des.byteArr2HexStr(rowkey));
		HttpClientUtil.postHttp(url + HBASE_URL_DELETE, paramMap);
	}
	
	
	
//	public static void main(String[] args) {
//		String content = searchMemory4json("http://114.215.249.43:8080/api/v1/clusters/gsj_fdp/hosts/iz231ph1zmyz");
//		Map<String,JSONObject> map = JSONObject.fromObject(content);
//		Map<String,JSONObject> map = json;
//		JSONObject diskInfos = map.get("Hosts");
//		System.out.println(diskInfos.get("disk_info"));
//		String disk = diskInfos.get("disk_info").toString();
//		parserToList(disk.substring(0,disk.length()));
		
//		String content = searchMemory4json("http://114.215.249.43:8080/api/v1/clusters/gsj_fdp/hosts/iz231ph1zmyz");
//		Map<String,JSONObject> map = JSONObject.fromObject(content);
//		Map<String,JSONObject> map = json;
//		JSONObject diskInfos = map.get("metrics");
//		System.out.println(diskInfos.get("cpu"));
//		String cpu = diskInfos.get("cpu").toString();
//		parserToMap(cpu);
		
//		String content = searchMemory4json("http://114.215.249.43:8080/api/v1/clusters/gsj_fdp/hosts/iz231ph1zmyz");
//		Map<String,JSONObject> map = JSONObject.fromObject(content);
//		JSONObject diskInfos = map.get("metrics");
//		System.out.println(diskInfos.get("memory"));
//		String memory = diskInfos.get("memory").toString();
//		parserToMap(memory);
//	}
}
