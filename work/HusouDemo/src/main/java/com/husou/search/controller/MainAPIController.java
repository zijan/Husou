package com.husou.search.controller;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.husou.entity.UnicomSearchRequest;
import com.husou.entity.UnicomSearchResult;
import com.husou.imports.Config;

@Controller
@EnableAutoConfiguration
public class MainAPIController {
	
	final static Logger logger = LoggerFactory.getLogger(MainAPIController.class);
	
	public static void main(String[] args) throws Exception {
        SpringApplication.run(MainAPIController.class, args);
    }
	
	@RequestMapping("/api/mainSearch")
    @ResponseBody
    public Map<String, Object> mainSearch(@RequestBody UnicomSearchRequest unicomSearchRequest) {
		Map<String, Object> resultMap = doMainSearch(unicomSearchRequest);
        return resultMap;
    }

    private Map<String, Object> doMainSearch(UnicomSearchRequest unicomSearchRequest){
    	logger.debug("doMainSearch~~~");

    	Map<String, Object> resultMap = new HashMap<String, Object>();
    	
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", Config.instance.getClusterName()).build();
		TransportClient client = new TransportClient(settings);
		client.addTransportAddress(new InetSocketTransportAddress(Config.instance.getEsHost(), 9300));

		String indexName = Config.instance.getIndexName();
		String typeName = Config.instance.getTypeName();
		
		//QueryStringQueryBuilder qb = QueryBuilders.queryStringQuery("飞机");
		RangeFilterBuilder fb = FilterBuilders.rangeFilter("createTime").from(unicomSearchRequest.getStartDate()).to(unicomSearchRequest.getEndDate());
		QueryBuilder qb = QueryBuilders.filteredQuery(QueryBuilders.termQuery("searchString", unicomSearchRequest.getQuery()), fb);

		DateHistogramBuilder abDate = AggregationBuilders.dateHistogram("aggs_date")
				.field("createTime")
				.interval(DateHistogram.Interval.DAY)
				.format("yyyy-MM-dd")
				.minDocCount(0)
				.extendedBounds(unicomSearchRequest.getStartDate(), unicomSearchRequest.getEndDate());
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
		for(SearchHit hit : sr.getHits()){
			UnicomSearchResult usr = new UnicomSearchResult();
			usr.setRid((String)hit.getSource().get("rid"));
			usr.setSearchString((String)hit.getSource().get("searchString"));
			usr.setArea((String)hit.getSource().get("area"));
			usr.setCreateTime((String)hit.getSource().get("createTime"));
			list.add(usr);
		}
		
		resultMap.put("SearchResultList", list);
		
		DateHistogram aggDate = sr.getAggregations().get("aggs_date");
		Terms aggArea = sr.getAggregations().get("aggs_area");
		
		for(Bucket bucket : aggDate.getBuckets()){
			logger.debug("["+bucket.getKeyAsText()+"]["+bucket.getDocCount()+"]");
		}
		
		for(Terms.Bucket bucket : aggArea.getBuckets()){
			logger.debug("["+bucket.getKeyAsText()+"]["+bucket.getDocCount()+"]");
		}
		
		client.close();
		
		return resultMap;
	}

}
