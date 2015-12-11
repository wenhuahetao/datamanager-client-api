package com.fline.hadoop.examples;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.junit.Test;

import com.fline.hadoop.task.TaskOperator;

public class TaskOperatorTest extends TestCase {

	final static String JOB_URL = "http://114.215.249.44:19888/ws/v1/history/mapreduce/jobs/job";
	
//	final static String JOB_URL = "http://fdp-master:19888/ws/v1/history/mapreduce/jobs/job"; 
 
//	final static String AMBARI_URL = "http://fdp-master:10001/api/v1/clusters/FDP/hosts/"; 
	
	final static String AMBARI_URL = "http://114.215.249.43:8080/api/v1/clusters/gsj_fdp/hosts/"; 
	
//	final static String URL = "http://fdp-master:8087/fline_hdfs"; 
	
	final static String URL = "http://localhost:8080/fline_hdfs"; 
	
//	final static String URL = "http://114.215.249.44:8084/fline_hdfs";  
	
	
	@Test
	public void testListJobIds() {
		String json = TaskOperator.listJobIds(URL);
		System.out.println(json);
	}
	
	@Test
	public void testSearchCurrentMonthDisk() {
		System.out.println(TaskOperator.searchCurrentMonthDisk(2015,11,URL));
	}
	
	@Test
	public void testSearchCurrentMonthMR() {
		System.out.println(TaskOperator.searchCurrentMonthMR(2015,11,URL));
	}
	
	@Test
	public void testSearchCurrentDayMR() {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:ms");
			long time = sdf.parse("2015-11-3 07:00:00").getTime();
			System.out.println(TaskOperator.searchCurrentDayMR(time,URL));
		} catch (ParseException e) {
			e.printStackTrace();
		} 
	}
	
	@Test
	public void testSearchCurrentDayDisk() {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:ms");
			long time = sdf.parse("2015-11-24 00:00:00").getTime();
			System.out.println(TaskOperator.searchCurrentDayDisk(time,URL));
		} catch (ParseException e) {
			e.printStackTrace();
		} 
	}
	
	@Test
	public void testRunMrShellJob() {
		String path = "D:/project/Work_summary/Big_data/JT_LinePredict_train.java";
		String mrkey = "train9";
		System.out.println(TaskOperator.runMrShellJob(path, "JT_LinePredict_train", mrkey,"/user/root/testdata.txt /user/root/output_18", URL));
	}
	
	@Test
	public void testCmdMrShellJob() {
		System.out.println(TaskOperator.runCmdShellJob("ls -ls", URL));
	}
	
	@Test
	
	public void testSearchJOB() {
		String jobid = "1447664128119_0002";
		System.out.println(TaskOperator.searchJob(URL,JOB_URL,jobid));
	}
	
	@Test
	public void testSearchNode4Count() {
		String jobid = "1447664128119_0002";
		System.out.println(TaskOperator.searchNode4Count(URL,JOB_URL,jobid));
	}

	@Test
	public void testSearchNode4Error() {
		String jobid = "1447664128119_0002";
		System.out.println(TaskOperator.searchNode4Error(URL,JOB_URL,jobid));
	}
	
	@Test
	public void testSearchNode4Ranking() {
		String jobid = "1447664128119_0002";
		System.out.println(TaskOperator.searchNode4Ranking(URL,JOB_URL,jobid));
	}
	
	@Test
	public void testSearchResourceCounter() {
		String jobid = "1447664128119_0002";
		System.out.println(TaskOperator.searchResourceCounter(URL,JOB_URL,jobid));
	}
	
	
	@Test
	public void testsearchAllResource4Host() {
		System.out.println(TaskOperator.searchAllResource4Host(AMBARI_URL,"iz231ph1zmyz")); 
	}
	
	@Test
	public void testSearchDiskInfo() {
		System.out.println(TaskOperator.searchDiskInfo(AMBARI_URL,"iz231ph1zmyz")); 
	}
	
	@Test
	public void testSearchMemory() {
		System.out.println(TaskOperator.searchMemory(AMBARI_URL,"iz231ph1zmyz"));
	}
	
	@Test
	public void testSearchCPU() {
		System.out.println(TaskOperator.searchCPU(AMBARI_URL,"iz231ph1zmyz"));
	}
		
}
