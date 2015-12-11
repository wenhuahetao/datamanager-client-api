package com.fline.hadoop.solr;

import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.json.JSONObject;
import com.fline.hadoop.httpClient.HttpClientUtil;
import com.fline.hadoop.util.EncryptionDecryption;
import com.fline.hadoop.util.RelationEnum;
import com.fline.hadoop.util.SearchBean;

/**
 * Solr operation class
 * @author hetao
 */
public class SolrOperator {

	final static String SOLR_URL_QUERY = "/solr/queryFromSolr";
	final static String SOLR_URL_QUERY_PAGE = "/solr/queryFromSolr4Page";
	final static String SOLR_URL_QUERY_FACET = "/solr/queryByFacet";

	public static String queryFromSolr4Page(List<SearchBean> searchBeanList,
			String queryFields, int page, int rows, String sortField,
			String order, String url) throws Exception {
		StringBuffer querystr = new StringBuffer();
		for (SearchBean searchBean : searchBeanList) {
			String name = searchBean.getName();
			String value = searchBean.getValue();
			RelationEnum relation = searchBean.getRelation();
			if (relation != null) {
				if (RelationEnum.LIKE.equals(relation)) {
					if (querystr.toString().endsWith(
							" " + RelationEnum.AND + " ")) {
						querystr.append(name + ":" + relation.getCode() + value
								+ relation.getCode() + " ");
					} else {
						querystr.append(" " + RelationEnum.AND + " " + name
								+ ":" + relation.getCode() + value
								+ relation.getCode() + " ");
					}
				} else {
					querystr.append(name + ":" + "\"" + value + "\" "
							+ relation.getCode() + " ");
				}
			} else {
				querystr.append(name + ":" + "\"" + value + "\" ");
			}
		}
		EncryptionDecryption des = new EncryptionDecryption("wf");
		Map<String, String> paramMap = new HashMap<String, String>();
		paramMap.put("queryTerms",
				des.encrypt(URLEncoder.encode(querystr.toString(), "GBK")));
		paramMap.put("queryFields", queryFields);
		paramMap.put("page", page + "");
		paramMap.put("rows", rows + "");
		paramMap.put("sortField", sortField);
		paramMap.put("order", order);
		String content = HttpClientUtil.getHttp(url + SOLR_URL_QUERY_PAGE,
				paramMap);
		JSONObject json = JSONObject.fromObject(content);
		return json.get("contents").toString();
	}

	public static String queryFromSolr(List<SearchBean> searchBeanList,
			String queryFields, int rows, String sortField, String order,
			String url) throws Exception {
		StringBuffer querystr = new StringBuffer();
		for (SearchBean searchBean : searchBeanList) {
			String name = searchBean.getName();
			String value = searchBean.getValue();
			RelationEnum relation = searchBean.getRelation();
			if (relation != null) {
				if (RelationEnum.LIKE.equals(relation)) {
					if (querystr.toString().endsWith(
							" " + RelationEnum.AND + " ")) {
						querystr.append(name + ":" + relation.getCode() + value
								+ relation.getCode() + " ");
					} else {
						querystr.append(" " + RelationEnum.AND + " " + name
								+ ":" + relation.getCode() + value
								+ relation.getCode() + " ");
					}
				} else {
					querystr.append(name + ":" + "\"" + value + "\" "
							+ relation.getCode() + " ");
				}
			} else {
				querystr.append(name + ":" + "\"" + value + "\" ");
			}
		}
		EncryptionDecryption des = new EncryptionDecryption("wf");
		Map<String, String> paramMap = new HashMap<String, String>();
		paramMap.put("queryTerms",
				des.encrypt(URLEncoder.encode(querystr.toString(), "UTF-8")));
		paramMap.put("queryFields", queryFields);
		paramMap.put("rows", rows + "");
		paramMap.put("sortField", sortField);
		paramMap.put("order", order);
		String content = HttpClientUtil.getHttp(url + SOLR_URL_QUERY, paramMap);
		JSONObject json = JSONObject.fromObject(content);
		return json.get("contents").toString();
	}

	public static String queryByStats(List<SearchBean> searchBeanList,
			String facetFields, String statsFields, String url)
			throws Exception {
		StringBuffer querystr = new StringBuffer();
		if (searchBeanList != null) {
			for (SearchBean searchBean : searchBeanList) {
				String name = searchBean.getName();
				String value = searchBean.getValue();
				RelationEnum relation = searchBean.getRelation();
				if (relation != null) {
					if (RelationEnum.LIKE.equals(relation)) {
						if (querystr.toString().endsWith(
								" " + RelationEnum.AND + " ")) {
							querystr.append(name + ":" + relation.getCode()
									+ value + relation.getCode() + " ");
						} else {
							querystr.append(" " + RelationEnum.AND + " " + name
									+ ":" + relation.getCode() + value
									+ relation.getCode() + " ");
						}
					} else {
						querystr.append(name + ":" + "\"" + value + "\" "
								+ relation.getCode() + " ");
					}
				} else {
					querystr.append(name + ":" + "\"" + value + "\" ");
				}
			}
		}
		EncryptionDecryption des = new EncryptionDecryption("wf");
		Map<String, String> paramMap = new HashMap<String, String>();
		paramMap.put("queryTerms",
				des.encrypt(URLEncoder.encode(querystr.toString(), "UTF-8")));
		paramMap.put("facetField", facetFields);
		paramMap.put("statsFields", statsFields);
		String content = HttpClientUtil.getHttp(url + SOLR_URL_QUERY_FACET,
				paramMap);
		return content;
	}

	public static List<Map<String, String>> queryByFacet(
			List<SearchBean> searchBeanList, String facetField, int rows,
			String url) throws Exception {
		StringBuffer querystr = new StringBuffer();
		if (searchBeanList != null) {
			for (SearchBean searchBean : searchBeanList) {
				String name = searchBean.getName();
				String value = searchBean.getValue();
				RelationEnum relation = searchBean.getRelation();
				if (relation != null) {
					if (RelationEnum.LIKE.equals(relation)) {
						if (querystr.toString().endsWith(
								" " + RelationEnum.AND + " ")) {
							querystr.append(name + ":" + relation.getCode()
									+ value + relation.getCode() + " ");
						} else {
							querystr.append(" " + RelationEnum.AND + " " + name
									+ ":" + relation.getCode() + value
									+ relation.getCode() + " ");
						}
					} else {
						querystr.append(name + ":" + "\"" + value + "\" "
								+ relation.getCode() + " ");
					}
				} else {
					querystr.append(name + ":" + "\"" + value + "\" ");
				}
			}
		}
		EncryptionDecryption des = new EncryptionDecryption("wf");
		Map<String, String> paramMap = new HashMap<String, String>();
		paramMap.put("queryTerms",
				des.encrypt(URLEncoder.encode(querystr.toString(), "UTF-8")));
		paramMap.put("facetField", facetField + "_d");
		paramMap.put("rows", rows + "");
		String content = HttpClientUtil.getHttp(url + SOLR_URL_QUERY_FACET,
				paramMap);
		JSONObject json = JSONObject.fromObject(content);
		List<Map<String, String>> list = (List<Map<String, String>>) json
				.get("contents");
		return list;
	}
}
