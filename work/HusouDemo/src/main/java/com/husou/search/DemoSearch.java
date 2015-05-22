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
		
		//QueryStringQueryBuilder qb = QueryBuilders.queryString("game").field("title");
		
		QueryBuilder qb = QueryBuilders.boolQuery()
				.must(QueryBuilders.queryString("游戏").field("searchString"));
		
		
		SearchResponse sr = client.prepareSearch(indexName).setTypes(typeName)
				.setQuery(qb).setSize(100)
				.execute().actionGet();
		for(SearchHit hit : sr.getHits()){
			//logger.debug("["+hit.getSource().get("bid")+"]["+hit.getSource().get("year")+"]["+hit.getSource().get("title")+"]");
			logger.debug("["+hit.getSource()+"]");
		}

		client.close();
	}

}
