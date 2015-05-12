package com.husou.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DemoSearch {
	final static Logger logger = LoggerFactory.getLogger(DemoSearch.class);

	public static void main(String[] args) {
		logger.debug("start~~~");

		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "JimTest").build();
		TransportClient client = new TransportClient(settings);
		client.addTransportAddress(new InetSocketTransportAddress("localhost", 9301));

		//QueryStringQueryBuilder qb = QueryBuilders.queryString("game").field("title");
		
		QueryBuilder qb = QueryBuilders.boolQuery()
				.must(QueryBuilders.queryString("game").field("title"))
				.must(QueryBuilders.queryString("2012").field("year"));
		
		
		SearchResponse sr = client.prepareSearch("hbindex4921").setTypes("heartbeat")
				.setQuery(qb).setSize(100)
				.execute().actionGet();
		for(SearchHit hit : sr.getHits()){
			logger.debug("["+hit.getSource().get("bid")+"]["+hit.getSource().get("year")+"]["+hit.getSource().get("title")+"]");
		}

		client.close();
	}

}
