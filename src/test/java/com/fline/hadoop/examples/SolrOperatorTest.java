package com.fline.hadoop.examples;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import junit.framework.TestCase;
import com.fline.hadoop.solr.SolrOperator;
import com.fline.hadoop.util.RelationEnum;
import com.fline.hadoop.util.SearchBean;

public class SolrOperatorTest extends TestCase {

//	final static String URL = "http://112.33.1.201:8087/fline_hdfs"; 
	final static String URL = "http://114.215.249.44:8084/fline_hdfs";  
//	final static String AMBARI_URL = "http://10.122.70.189:8080/api/v1/clusters/gsj_fdp/hosts/iz231ph1zmyz"; 
//	final static String URL = "http://localhost:8080/fline_hdfs"; 
 
	@Test
	public void testQueryFromSolr() { 
		try {
			List<SearchBean> searchBeanList = new ArrayList<SearchBean>();
			searchBeanList.add(new SearchBean("sourceType", "2", RelationEnum.AND));
			searchBeanList.add(new SearchBean("rdbtablename", "ENTERPRISEINFO",null));
			searchBeanList.add(new SearchBean("EntName_d", "杭州",RelationEnum.LIKE));
			
			String sortField = "createdTime";
			String order = "desc";
			int page = 3;
			int rows = 10;
			String queryFields = "Dom,RegCap,ConForm";
			String str = SolrOperator.queryFromSolr4Page(searchBeanList, queryFields, page,rows, sortField, order, URL);
			System.out.println(str);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
//	@Test
//	public void testQueryByFacet() { 
//		try {
//			List<SearchBean> searchBeanList = new ArrayList<SearchBean>();
//			searchBeanList.add(new SearchBean("sourceType", "2", RelationEnum.AND));
//			searchBeanList.add(new SearchBean("rdbtablename", "ENTERPRISEINFO",RelationEnum.AND));
//			searchBeanList.add(new SearchBean("Dom_d", "ds",RelationEnum.LIKE));
////			searchBeanList.add(new SearchBean("RegNO_d", "568006",RelationEnum.LIKE));
//			int rows = 100;
//			String queryFields = "Dom";
//			List<Map<String,String>> list = SolrOperator.queryByFacet(searchBeanList, queryFields, rows, URL);
//			for (Map<String, String> map : list) {
//				String fieldVal = map.get("fieldValue");
//				String facetField = map.get("count");
//				System.out.println(fieldVal+":"+facetField);
//			}
//			
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
}
