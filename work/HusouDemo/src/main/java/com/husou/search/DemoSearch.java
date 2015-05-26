package com.husou.search;

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

import com.husou.imports.Config;

public class DemoSearch {
	final static Logger logger = LoggerFactory.getLogger(DemoSearch.class);

	public static void main(String[] args) {
		logger.debug("start~~~");

		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", Config.instance.getClusterName()).build();
		TransportClient client = new TransportClient(settings);
		client.addTransportAddress(new InetSocketTransportAddress(Config.instance.getEsHost(), 9300));

		String indexName = Config.instance.getIndexName();
		String typeName = Config.instance.getTypeName();
		
		//QueryStringQueryBuilder qb = QueryBuilders.queryStringQuery("飞机");
		RangeFilterBuilder fb = FilterBuilders.rangeFilter("createTime").from("2015-04-26").to("2015-05-01");
		QueryBuilder qb = QueryBuilders.filteredQuery(QueryBuilders.termQuery("searchString", "飞机"), fb);

		DateHistogramBuilder abDate = AggregationBuilders.dateHistogram("aggs_date")
				.field("createTime")
				.interval(DateHistogram.Interval.DAY)
				.format("yyyy-MM-dd")
				.minDocCount(0)
				.extendedBounds("2015-04-26", "2015-05-01");
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
		logger.debug("TotalHits: ["+sr.getHits().getTotalHits()+"]");
		for(SearchHit hit : sr.getHits()){
			logger.debug("["+hit.getSource()+"]");
		}
		
		DateHistogram aggDate = sr.getAggregations().get("aggs_date");
		Terms aggArea = sr.getAggregations().get("aggs_area");
		
		for(Bucket bucket : aggDate.getBuckets()){
			logger.debug("["+bucket.getKeyAsText()+"]["+bucket.getDocCount()+"]");
		}
		
		for(Terms.Bucket bucket : aggArea.getBuckets()){
			logger.debug("["+bucket.getKeyAsText()+"]["+bucket.getDocCount()+"]");
		}
		

		client.close();
	}

}
