package com.fline.hadoop.task;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import com.fline.hadoop.httpClient.HttpClientUtil;
import com.fline.hadoop.util.EncryptionDecryption;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;

public class TaskOperator {

	final static String JOB = "";
	final static String JOBATTEMPTS = "/jobattempts";
	final static String TASKS = "/tasks";
	final static String COUNTERS = "/counters";
	final static String ATTEMPTS = "/attempts";
	final static String SEARCH_TASK_COUNTER = "/collection/searchTaskCounter";
	
	final static String SEARCH_CURRENT_DAY_DISK = "/collection/searchCurrentDayDisk";
	final static String SEARCH_CURRENT_DAY_MR = "/collection/searchCurrentDayMR";
	final static String SEARCH_CURRENT_MONTH_MR = "/collection/searchCurrentMonthMR";
	final static String SEARCH_CURRENT_MONTH_DISK = "/collection/searchCurrentMonthDisk";
	final static String LIST_JOB_IDS = "/collection/listJobIds";
	final static String RUN_MR_SHELL_JOB = "/collection/runMrShellJob";
	final static String RUN_MR_MKDIR = "/collection/runMrWithMkDir";
	final static String RUN_CMD_SHELL_JOB = "/collection/runCmdShellJob";
	final static String QUERY_TASK_MAP = "/collection/queryTaskMap";
	
	final static String DISK_INFO = "?fields=Hosts/disk_info";
	final static String CPU = "?fields=metrics/cpu";
	final static String MEMORY = "?fields=metrics/memory";
	
	
	/**
	 * 返回作业列表的接口
	 * @param url 大数据url
	 * @return
	 */
	public static String listJobIds(String url) {
		Map<String,String> paramMap = new HashMap<String,String>();
		String json = HttpClientUtil.getHttp(url + LIST_JOB_IDS, paramMap);
		Map<String,Map<String,Object>> jsonObj = JSONObject.fromObject(json);
		return jsonObj.toString();
	}
	
	/**
	 * 获取月作业增量
	 * @param year 年份
	 * @param moth 月份
	 * @param url 大数据url
	 * @return
	 */
	public static String searchCurrentMonthDisk(int year,int moth,String url) {
		String currentMonthDate = year + "-" + moth;
		Map<String,String> paramMap = new HashMap<String,String>();
		paramMap.put("currentMonthDate",currentMonthDate);
		String json = HttpClientUtil.getHttp(url + SEARCH_CURRENT_MONTH_DISK, paramMap);
		return json;
	}
	
	/**
	 * 获取月资源增量
	 * @param year 年份
	 * @param moth 月份
	 * @param url 大数据url
	 * @return
	 */
	public static String searchCurrentMonthMR(int year,int moth,String url) {
		String currentMonthDate = year + "-" + moth;
		Map<String,String> paramMap = new HashMap<String,String>();
		paramMap.put("currentMonthDate",currentMonthDate);
		String json = HttpClientUtil.getHttp(url + SEARCH_CURRENT_MONTH_MR, paramMap);
		return json;
	}
	
	/**
	 * 获取单日资源增量
	 * @param time 时间
	 * @param url 大数据url
	 * @return
	 */
	public static String searchCurrentDayMR(long time,String url) {
		Map<String,String> paramMap = new HashMap<String,String>();
		paramMap.put("time",time + "");
		String json = HttpClientUtil.getHttp(url + SEARCH_CURRENT_DAY_MR, paramMap);
		return json;
	}
	
	/**
	 * 获取单日存储增量
	 * @param time 时间
	 * @param url 大数据url
	 * @return 返回的是当天和历史的数据，相减即为增量
	 */
	public static String searchCurrentDayDisk(long time,String url) {
		Map<String,String> paramMap = new HashMap<String,String>();
		paramMap.put("time",time + "");
		String json = HttpClientUtil.getHttp(url + SEARCH_CURRENT_DAY_DISK, paramMap);
		return json;
	}
	
	/**
	 * mapreduce 命令
	 * @param localpath 文件路径
	 * @param className class类名
	 * @param mrkey 用户自定义的唯一名称，执行的MR日志根据它生成
	 * @param excuParma 命令参数
	 * @param url 大数据url
	 * @return
	 */
	public static String runMrShellJob(String localpath,String className,String mrkey,String excuParma,String url) {
		String runDir = runMrWithMkDir(url);
		String webfilePath = "";
		if(!"error".equals(runDir)){
			String remoteDir = url.substring(0,url.lastIndexOf("/")) + "/image-web/upload" + runDir;
			webfilePath = remoteDir + localpath.substring(localpath.lastIndexOf("/"),localpath.length());
			uploadMRFile(webfilePath,localpath);
		}
		Map<String,String> paramMap = new HashMap<String,String>();
		paramMap.put("runDir",runDir);
		paramMap.put("className",className);
		paramMap.put("mrkey",mrkey);
		paramMap.put("excuParma",excuParma);
		String json = HttpClientUtil.postHttp(url + RUN_MR_SHELL_JOB, paramMap);
		return json;
	}
	
	private static String runMrWithMkDir(String url) {
		Map<String,String> paramMap = new HashMap<String,String>();
		String json = HttpClientUtil.postHttp(url + RUN_MR_MKDIR, paramMap);
		return json;
	}
	
	public static boolean uploadMRFile(String webfilePath,String localpath) {
		try {
			Client client = new Client();
			WebResource resource = client.resource(webfilePath);
			byte[] readFileToByteArray = FileUtils.readFileToByteArray(new File(localpath));
			resource.put(String.class, readFileToByteArray);
		} catch (UniformInterfaceException e) {
			e.printStackTrace();
		} catch (ClientHandlerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}
	
	/**
	 * shell命令
	 * @param excuParma 命令参数
	 * @param url 大数据url
	 * @return
	 */
	public static String runCmdShellJob(String excuParma,String url) {
		try {
			Map<String,String> paramMap = new HashMap<String,String>();
			EncryptionDecryption des = new EncryptionDecryption("wf");
			paramMap.put("excuParma",des.encrypt(excuParma));
			String json = HttpClientUtil.getHttp(url + RUN_CMD_SHELL_JOB, paramMap);
			return json;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";
	}
	
	/**
	 * 作业资源消耗总量
	 * @param bigUrl 大数据url
	 * @param url yarn作业访问地址
	 * @param jobid job id
	 * @return
	 */
	public static String searchResourceCounter(String bigUrl,String url,String jobid) {
		return queryTaskMap("searchResourceCounter",jobid,bigUrl);
//		Map<String,String> paramMap = new HashMap<String,String>();
//		paramMap.put("applicationId","application_"+appId);
//		String json = HttpClientUtil.getHttp(url + SEARCH_TASK_COUNTER, paramMap);
//		return json;
	}
	
	/**
	 * 查询作业信息
	 * @param bigUrl 大数据url
	 * @param url yarn作业访问地址
	 * @param jobid job id
	 * @return
	 */
	public static String searchJob(String bigUrl,String url,String jobid) {
		return queryTaskMap("searchJob",jobid,bigUrl);
//		String http = HttpClientUtil.getHttp(url + "_" + jobid + JOB, new HashMap<String, String>());
//		JSONObject jsonObj = JSONObject.fromObject(http);
//		JSONObject jo = (JSONObject) jsonObj.get("job");
//		StringBuffer infoBuffer = new StringBuffer();
//		Map<String,String> map = new HashMap<String,String>();
//		infoBuffer.append(jo.get("state"));
//		map.put(jo.get("id").toString(), infoBuffer.toString());
//		String json = JSONObject.fromObject(map).toString();
//		return json;
	}
	
	/**
	 * 作业运行的节点数
	 * @param bigUrl 大数据平台url
	 * @param url yarn作业访问地址
	 * @param jobid 作业id
	 * @return
	 */
	public static String searchNode4Count(String bigUrl,String url,String jobid) {
		return queryTaskMap("searchNode4Count",jobid,bigUrl);
//		Map<String, List<String>> searchTaskAttempts = searchTaskAttempts(url,jobid);
//		return searchTaskAttempts.size() + "";
	}
	
	/**
	 * 异常节点数
	* @param bigUrl 大数据平台url
	 * @param url yarn作业访问地址
	 * @param jobid 作业id
	 * @return
	 */
	public static String searchNode4Error(String bigUrl,String url,String jobid) {
		return queryTaskMap("searchNode4Error",jobid,bigUrl);
//		Map<String, List<String>> searchTaskAttempts = searchTaskAttempts(url,jobid);
//		Map<String, List<String>> errorTaskAttempts = new HashMap<String,List<String>>();
//		for (String key : searchTaskAttempts.keySet()) {
//			List<String> list = searchTaskAttempts.get(key);
//			boolean errorFlag = false;
//			for (String info : list) {
//				String[] infoSplit = info.split(",");
//				if(infoSplit[1] != null && !infoSplit[1].equals("SUCCEEDED")){
//					errorFlag = true;
//				}
//			}
//			if(errorFlag){
//				errorTaskAttempts.put(key, list);
//			}
//		}
//		String json = JSONObject.fromObject(errorTaskAttempts).toString();
//		return json;
	}
	
	/**
	 * 查询节点排名
	* @param bigUrl 大数据平台url
	 * @param url yarn作业访问地址
	 * @param jobid 作业id
	 * @return
	 */
	 
	public static String searchNode4Ranking(String bigUrl,String url,String jobid) {
		
		return queryTaskMap("searchNode4Ranking",jobid,bigUrl);
		
//		Map<String,Long> rankMap = new HashMap<String,Long>();
//		Map<String, List<String>> searchTaskAttempts = searchTaskAttempts(url,jobid);
//		if(searchTaskAttempts == null){
//			return queryTaskMap("searchNode4Ranking",jobid,url);
//		}
//		for (String key : searchTaskAttempts.keySet()) {
//			List<String> list = searchTaskAttempts.get(key);
//			Long elapsedTime = 0L;
//			for (String info : list) {
//				String[] infoSplit = info.split(",");
//				if(infoSplit[0] != null){
//					elapsedTime += Long.parseLong(infoSplit[0]);
//				}
//			}
//			rankMap.put(key, elapsedTime);
//		}
//        List<Map.Entry<String,Long>> list = new ArrayList<Map.Entry<String,Long>>(rankMap.entrySet());
//        Collections.sort(list,new Comparator<Map.Entry<String,Long>>() {
//            //升序排序
//            public int compare(Entry<String, Long> o1,
//                    Entry<String, Long> o2) {
//                return o1.getValue().compareTo(o2.getValue());
//            }
//        });
//       
//        String json = JSONArray.fromObject(list).toString();
//        return json;
	}
	
	//获取每个作业节点信息
	private static Map<String,List<String>> searchTaskAttempts(String url,String jobid) {
		Map<String,String> taskMap = searchTask(url,jobid);
		if(taskMap == null){
			return null;
		}
		Map<String,List<String>> taskAttemptMap = new HashMap<String,List<String>>();
		System.out.println(taskMap.size());
		for (String key : taskMap.keySet()) {
			String httpUrl = url + "_" + jobid + TASKS;
			httpUrl += "/" + key + "/attempts";
			String http = HttpClientUtil.getHttp(httpUrl, new HashMap<String, String>());
			JSONObject jsonObj = JSONObject.fromObject(http);
			if(jsonObj != null){
				JSONObject jo = null;
				if(jsonObj.get("taskAttempts") != null && !"null".equals(jsonObj.get("taskAttempts").toString())){
					jo = (JSONObject) jsonObj.get("taskAttempts");
				}
				if(jo != null){
					JSONArray ja = (JSONArray) jo.get("taskAttempt");
					for (int i = 0; i < ja.size(); i++) {
						JSONObject taskAttempt = (JSONObject) ja.get(i);
						String id = (String) taskAttempt.get("nodeHttpAddress");
						StringBuffer infoBuff = new StringBuffer();
						infoBuff.append(taskAttempt.get("elapsedTime")).append(",").append(taskAttempt.get("state")).append(",").append(taskAttempt.get("type"));
						List<String> taskAttemptList = taskAttemptMap.get(id);
						if(taskAttemptList==null){
							taskAttemptList = new ArrayList<String>();
						}else{
							taskAttemptList = taskAttemptMap.get(id);
						}
						taskAttemptList.add(infoBuff.toString());
						taskAttemptMap.put(id, taskAttemptList);
					}
				}
			}
		}
		return taskAttemptMap;
	}
	
	//获取每个作业节点信息 资源消耗总量 
	private static String searchTaskCounter(String url,String jobid) {
		String http = HttpClientUtil.getHttp(url + "_" + jobid + COUNTERS, new HashMap<String, String>());
		JSONObject jsonObj = JSONObject.fromObject(http);
		JSONObject jo = (JSONObject) jsonObj.get("jobCounters");
		JSONArray ja = (JSONArray) jo.get("counterGroup");
		for (int i = 0; i < ja.size(); i++) {
			JSONObject taskAttempt = (JSONObject) ja.get(i);
			String counterGroupName = (String) taskAttempt.get("counterGroupName");
			if("org.apache.hadoop.mapreduce.JobCounter".equals(counterGroupName)){
				JSONArray counterArr = (JSONArray) taskAttempt.get("counter");
				Long vm = 0L;
				Long mm = 0L;
				for (int j = 0; j < counterArr.size(); j++) {
					JSONObject counter = (JSONObject) counterArr.get(j);
					String name = (String) counter.get("name");
					
					if("VCORES_MILLIS_MAPS".equals(name)){
						Long totalCounterValue = Long.parseLong(counter.get("totalCounterValue").toString());
						vm += totalCounterValue/1000;
					}
					if("VCORES_MILLIS_REDUCES".equals(name)){
						Long totalCounterValue = Long.parseLong(counter.get("totalCounterValue").toString());
						vm += totalCounterValue/1000;
					}
					if("MB_MILLIS_MAPS".equals(name)){
						Long totalCounterValue = Long.parseLong(counter.get("totalCounterValue").toString());
						mm += totalCounterValue/1000;
					}
					if("MB_MILLIS_REDUCES".equals(name)){
						Long totalCounterValue = Long.parseLong(counter.get("totalCounterValue").toString());
						mm += totalCounterValue/1000;
					}
				}
				return  mm + " MB-seconds, " + vm  + " vcore-seconds";
			}
		}
		return null;
	}
	
	//统计作业耗时（每个Task） 
	public static Map<String,String> searchTask(String url,String jobid) {
		String http = HttpClientUtil.getHttp(url + "_" + jobid + TASKS, new HashMap<String, String>());
		JSONObject jsonObj = JSONObject.fromObject(http);
		JSONObject jo = (JSONObject) jsonObj.get("tasks");
		if(jo != null){
			JSONArray ja = (JSONArray) jo.get("task");
			Map<String,String> map = new HashMap<String,String>();
			for (int i = 0; i < ja.size(); i++) {
				JSONObject task = (JSONObject) ja.get(i);
				String id = (String) task.get("id");
				StringBuffer infoBuff = new StringBuffer();
				infoBuff.append(task.get("elapsedTime")).append(",").append(task.get("type")).append(",").append(task.get("state"));
				map.put(id, infoBuff.toString());
			}
			return map;
		}else{
			return null;
		}
	
	}
	
	public static int searchJobattempts(String url,String jobid) {
		String http = HttpClientUtil.getHttp(url + "_" + jobid + JOBATTEMPTS, new HashMap<String, String>());
		JSONObject jsonObj = JSONObject.fromObject(http);
		JSONObject jo = (JSONObject) jsonObj.get("jobAttempts");
		JSONArray ja = (JSONArray) jo.get("jobAttempt");
		for (int i = 0; i < ja.size(); i++) {
			JSONObject jobattempt = ja.getJSONObject(i);
			String nodeId = (String) jobattempt.get("nodeId");
			System.out.println(jobattempt.get("nodeId")); 
		}
		return ja.size();
	}
	
	public static String searchTask4json(String url,String jobid) {
		Map<String,String> map = searchTask(url,jobid);
		String json = JSONObject.fromObject(map).toString();
		return json; 
	}
	
	/**
	 * 获取节点资源信息
	 * @param url 监控平台地址
	 * @param hosts 节点
	 * @return
	 */
	public static String searchAllResource4Host(String url,String hosts) {
		url = url + hosts;
		Map<String, String> paramMap = new HashMap<String, String>();
		String content = HttpClientUtil.getHttp(url, paramMap);
		return JSONObject.fromObject(content).toString();
	}
	
	
	/**
	 * 获取节点的磁盘信息
	 * @param url 监控平台地址
	 * @param hosts 节点
	 * @return
	 */
	public static List<Map<String,String>> searchDiskInfo(String url,String hosts) {
		url = url + hosts;
		String content = searchDiskInfo4json(url);
		Map<String,JSONObject> map = JSONObject.fromObject(content);
		JSONObject diskInfos = map.get("Hosts");
		String diskJson = diskInfos.get("disk_info").toString();
		System.out.println("diskJson:"+diskJson);
		return parserToList(diskJson);
	}
	
	/**
	 * 获取节点的内存信息
	 * @param url 监控平台地址
	 * @param hosts 节点
	 * @return
	 */
	public static Map<String,String> searchMemory(String url,String hosts) {
		url = url + hosts;
		String content = searchMemory4json(url);
		Map<String,JSONObject> map = JSONObject.fromObject(content);
		JSONObject diskInfos = map.get("metrics");
		String memoryJson = diskInfos.get("memory").toString();
		System.out.println("memoryJson:"+memoryJson);
		return parserToMap(memoryJson);
	}
	
	/**
	 * 获取节点的CPU信息
	 * @param url 监控平台地址
	 * @param hosts 节点
	 * @return
	 */
	public static Map<String,String> searchCPU(String url,String hosts) {
			url = url + hosts;
			String content = searchCPU4json(url);
			Map<String,JSONObject> map = JSONObject.fromObject(content);
			JSONObject diskInfos = map.get("metrics");
			String cpuJson = diskInfos.get("cpu").toString();
			System.out.println("cpuJson:"+cpuJson);
			return parserToMap(cpuJson);
	}
	
	private static Map<String, String> parserToMap(String s) {
			Map<String, String> map = new HashMap<String, String>();
			JSONObject jsonObj = JSONObject.fromObject(s);
			Iterator keys = jsonObj.keys();
			while (keys.hasNext()) {
				String key = (String) keys.next();
				String value = jsonObj.get(key).toString();
				map.put(key, value);
			}
		return map;
	}
	
	private static List<Map<String,String>> parserToList(String s){
		 	List<Map<String,String>> list = new ArrayList<Map<String,String>>();
			JSONArray json = JSONArray.fromObject(s);
			for (Object object : json) {
				Map<String,String> map = new HashMap<String,String>();
				JSONObject jsonObj = JSONObject.fromObject(object);
				Iterator keys=jsonObj.keys();
				while(keys.hasNext()){
					String key=(String) keys.next();
					String value=jsonObj.get(key).toString();
					map.put(key, value);
				}
				list.add(map);
			}
			return list;
	}
	 
	private static String searchDiskInfo4json(String url) {
			Map<String, String> paramMap = new HashMap<String, String>();
			return HttpClientUtil.getHttp(url + DISK_INFO, paramMap);
	}

	private static String searchCPU4json(String url) {
			Map<String, String> paramMap = new HashMap<String, String>();
			return HttpClientUtil.getHttp(url + CPU, paramMap);
	}

	private static String searchMemory4json(String url) {
		Map<String, String> paramMap = new HashMap<String, String>();
		return HttpClientUtil.getHttp(url + MEMORY, paramMap);
	}
	
	public static void searchJobs(String url,String bigUrl){
		String http = HttpClientUtil.getHttp(url.substring(0,url.lastIndexOf("/")), new HashMap<String, String>());
		JSONObject jsonObj = JSONObject.fromObject(http);
		JSONObject jo = (JSONObject) jsonObj.get("jobs");
		JSONArray ja = (JSONArray) jo.get("job");
		Map<String,Map<String,Object>> jobsMap = new HashMap<String,Map<String,Object>>();
		for (int i = 0; i < ja.size(); i++) {
			Map<String,Object> jobMap= new HashMap<String,Object>();
			JSONObject jobattempt = ja.getJSONObject(i);
			String jobId = (String) jobattempt.get("id");
			jobId = jobId.replace("job_", "");
			jobMap.put("searchJob", searchJob(bigUrl,url, jobId));
			jobMap.put("searchNode4Error", searchNode4Error(bigUrl,url, jobId));
			jobMap.put("searchNode4Ranking", searchNode4Ranking(bigUrl,url, jobId));
			jobMap.put("searchResourceCounter", searchResourceCounter(bigUrl,url,jobId));
			jobsMap.put(jobId, jobMap);
		}
	}
	
	//queryTaskMap
	private static String queryTaskMap(String excuParma,String jobId,String url) {
		try {
			Map<String,String> paramMap = new HashMap<String,String>();
			paramMap.put("excuParma",excuParma);
			paramMap.put("jobId",jobId);
			String json = HttpClientUtil.getHttp(url + QUERY_TASK_MAP, paramMap);
			return json;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";
	}
		
	public static void main(String[] args) {
		HashMap<String,String> aa = new HashMap<String,String>();
		aa.put("submitTime", "1447328966387");
		aa.put("startTime", "1447328973739");
		System.out.println(aa);
	}
}
