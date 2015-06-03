package com.husou.search.controller;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Bucket;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.husou.entity.UnicomSearchResult;
import com.husou.imports.Config;
import com.husou.util.JsonUtil;

@Controller
@EnableAutoConfiguration
public class MainAPIController {
	
	final static Logger logger = LoggerFactory.getLogger(MainAPIController.class);
	
	public static void main(String[] args) throws Exception {
        SpringApplication.run(MainAPIController.class, args);
        //MainAPIController mapi = new MainAPIController();
        //mapi.test();
    }
	
	@RequestMapping("/api/mainSearch")
    @ResponseBody
    public String mainSearch(HttpServletRequest request) {
		String query = request.getParameter("query");
		String startDate = request.getParameter("startDate");
		String endDate = request.getParameter("endDate");
		String result = doMainSearch(query, startDate, endDate);
        return result;
    }

    private String doMainSearch(String query, String startDate, String endDate){
    	logger.debug("doMainSearch~~~");

    	Map<String, Object> resultMap = new LinkedHashMap<String, Object>();
    	
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", Config.instance.getClusterName()).build();
		TransportClient client = new TransportClient(settings);
		client.addTransportAddress(new InetSocketTransportAddress(Config.instance.getEsHost(), 9300));

		String indexName = Config.instance.getIndexName();
		String typeName = Config.instance.getTypeName();
		
		//QueryStringQueryBuilder qb = QueryBuilders.queryStringQuery("飞机");
		RangeFilterBuilder fb = FilterBuilders.rangeFilter("createTime").from(startDate).to(endDate);
		QueryBuilder qb = QueryBuilders.filteredQuery(QueryBuilders.termQuery("searchString", query), fb);

		DateHistogramBuilder abDate = AggregationBuilders.dateHistogram("aggs_date")
				.field("createTime")
				.interval(DateHistogram.Interval.DAY)
				.format("yyyy-MM-dd")
				.minDocCount(0)
				.extendedBounds(startDate, endDate);
		TermsBuilder abArea = AggregationBuilders.terms("aggs_area").field("area").size(5);
		
		FieldSortBuilder sb = SortBuilders.fieldSort("createTime").order(SortOrder.DESC);
		
		SearchResponse sr = client.prepareSearch(indexName).setTypes(typeName)
				.setQuery(qb)
				.addAggregation(abDate)
				.addAggregation(abArea)
				.addSort(sb)
				.setSize(10)
				.execute()
				.actionGet();
		
		List<UnicomSearchResult> list = new ArrayList<UnicomSearchResult>();
		
		logger.debug("TotalHits: ["+sr.getHits().getTotalHits()+"]");
		resultMap.put("Total", sr.getHits().getTotalHits());
		
		for(SearchHit hit : sr.getHits()){
			UnicomSearchResult usr = new UnicomSearchResult();
			usr.setRid((String)hit.getSource().get("rid"));
			usr.setSearchString((String)hit.getSource().get("searchString"));
			usr.setArea((String)hit.getSource().get("area"));
			usr.setCreateTime((String)hit.getSource().get("createTime"));
			
			String sentimentPath = "http://api.nlp.qq.com/text/sentiment";
			String sentimentRequestBody = "{\"content\":\""+usr.getSearchString()+"\"}";
			String positiveStr = nlpQQCall(sentimentPath, sentimentRequestBody).get("positive")+"";
			usr.setSentiment(positiveStr);
			
			sentimentPath = "http://api.nlp.qq.com/text/classify";
			sentimentRequestBody = "{\"content\":\""+usr.getSearchString()+"\"}";
			String classify = ((List<Map<String, String>>)nlpQQCall(sentimentPath, sentimentRequestBody).get("classes")).get(0).get("class");
			try {
				classify = new String(classify.getBytes(), "utf-8");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			usr.setClassify(classify);
			
			list.add(usr);
		}
		resultMap.put("SearchResultList", list);
		
		DateHistogram aggDate = sr.getAggregations().get("aggs_date");
		Map<String, Integer> aggDateMap = new LinkedHashMap<String, Integer>();
		for(Bucket bucket : aggDate.getBuckets()){
			logger.debug("["+bucket.getKeyAsText()+"]["+bucket.getDocCount()+"]");
			aggDateMap.put(bucket.getKeyAsText().toString(), (int)bucket.getDocCount());
		}
		resultMap.put("AggDate", aggDateMap);
		
		Terms aggArea = sr.getAggregations().get("aggs_area");
		Map<String, Integer> aggAreaMap = new LinkedHashMap<String, Integer>();
		for(Terms.Bucket bucket : aggArea.getBuckets()){
			logger.debug("["+bucket.getKeyAsText()+"]["+bucket.getDocCount()+"]");
			aggAreaMap.put(bucket.getKeyAsText().toString(), (int)bucket.getDocCount());
		}
		resultMap.put("AggArea", aggAreaMap);
		
		client.close();

		return JsonUtil.prettyPrint(resultMap);
	}

    private Map<String, Object> nlpQQCall(String path, String requestBody){
    	Map<String, Object> resultMap = null;
    	try {
    		 
    		CloseableHttpClient httpClient = HttpClientBuilder.create().build();
    		
    		//HttpPost postRequest = new HttpPost("http://api.nlp.qq.com/text/sentiment");
    		HttpPost postRequest = new HttpPost(path);
    	 
    		//StringEntity input = new StringEntity("{\"content\":\"双万兆服务器就是好，只是内存小点\"}", ContentType.APPLICATION_JSON);
    		StringEntity input = new StringEntity(requestBody, ContentType.APPLICATION_JSON);
			input.setContentType("application/json");
			postRequest.setEntity(input);
			postRequest.setHeader("Accept", "application/json");
			postRequest.setHeader("Content-Type", "application/json");
			postRequest.setHeader("S-Token", Config.instance.getProperty("token"));
			postRequest.setHeader("S-Openid", "37287685");
	 
			HttpResponse response = httpClient.execute(postRequest);
	 
			if (response.getStatusLine().getStatusCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatusLine().getStatusCode());
			}
	 
			BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));
	 
			StringBuffer result = new StringBuffer();
			String line = "";
			while ((line = br.readLine()) != null) {
				result.append(line);
			}
			
			logger.debug("result: "+result);
 
			resultMap = JsonUtil.deserialize(result.toString());
			
		    httpClient.close();
     
    	  } catch (Exception e) {
    		e.printStackTrace();
    	  }
    	
    	return resultMap;
    }
}
