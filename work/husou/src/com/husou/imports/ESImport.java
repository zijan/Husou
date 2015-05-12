package com.husou.imports;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

public class ESImport extends Thread{
	private Logger logger = Logger.getLogger(this.getClass());
	private Config config;
    private Connection conn;
    private int yearBase = 1900;
    private TransportClient client;
    
    private String indexName = "hbindex";
    private String typename = "heartbeat";
    
	public ESImport(Config config) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        conn = DriverManager.getConnection(config.getDbUrl(), config.getDbUser(), config.getDbPass());
        this.config = config;
        
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", config.getClusterName()).build();
        client = new TransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(config.getEsHost(), 9300));
        
    }
	
	public void run() {
		int hid = config.getHid();
        logger.info("started thread for hid:" + hid + "\t batch size:" + config.getBatchSize());
        String beatSql = "select b.*, t.taglist FROM beats_hb" + hid + " b, taglist_hb" + hid + " t "
                + " where b.bid=t.bid "
                + " and b.bid > ?  "
                + " order by b.bid asc "
                + " limit " + config.getBatchSize();

        PreparedStatement stat = null;
        ResultSet rs = null;
        
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        int storedBid = -1;
        
        //check index exist
        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet(); 
        ClusterStateResponse response = client.admin().cluster().prepareState().execute().actionGet(); 
        boolean hasIndex = response.getState().metaData().hasIndex(indexName+hid);
        if(hasIndex){
        	//find last bid
        	SearchRequestBuilder builder= client.prepareSearch(indexName+hid)  
                    .setTypes(typename)  
                    .setSearchType(SearchType.DEFAULT)  
                    .setFrom(0)  
                    .setSize(1)
                    .addSort(SortBuilders.fieldSort("bid").order(SortOrder.DESC)); 

            SearchResponse searchResponse = builder.execute().actionGet();
            SearchHits hits = searchResponse.getHits();
            
            if(hits.totalHits() < 1){
            	storedBid = 0;
            }else{
            	int bid = (int)hits.getAt(0).getSource().get("bid");
            	storedBid = bid;
            }
        }else{
        	storedBid = 0;
        }
        
        
        try {
            while (true) {
                try {

                	long timeflag = System.currentTimeMillis();
                	
                    stat = conn.prepareStatement(beatSql);
                    stat.setInt(1, storedBid);

                    logger.info("\n" + stat);
                    rs = stat.executeQuery();

                    boolean empty = true;
                    int i = 0;
                    while (rs.next()) {
                        i++;
                        empty = false;
                        Map<String, Object> fields = rowToMap(rs);
                        
                        index(indexName+hid, bulkRequest, fields);
                        
                        storedBid = rs.getInt("bid");
                    }
                    
                    if(!empty){
                    	BulkResponse bulkResponse = bulkRequest.execute().actionGet(); 
                        bulkRequest = client.prepareBulk();
            	        if (bulkResponse.hasFailures()) { 
            	            logger.debug(bulkResponse.buildFailureMessage());
            	        }
            	        
            	        long time = (System.currentTimeMillis() - timeflag)/1000;
                        logger.info("added " + i + " docs for hid:" + hid + " [takes: "+time+"s]");
                        if (!config.isKeepRunning()) {
                            logger.info("Not in keep running mode. job finished");
                            break;
                        }
                    } else {
                        int waitInterval = config.getWaitInterval();
                        logger.info("wait " + waitInterval + " minutes for next fetch for hid:" + hid);
                        this.sleep(1000 * 60 * waitInterval);
                    }

                } catch (Exception ex) {
                    ex.printStackTrace();
                    logger.error("Exception for hid:" + hid, ex);
                    continue;
                } finally {
                    logger.info("completed one batch for hid:" + hid);
                }
            }
        } finally {
            logger.info("**************completed thread for hid:" + hid + "*******************");
            try {
                rs.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                stat.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                conn.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            client.close();
        }
	}
	
    private Map<String, Object> rowToMap(ResultSet rs) throws SQLException {

        Map<String, Object> fields = new LinkedHashMap<String, Object>();

        String refid = rs.getString("refid");
        Date postTime = rs.getDate("posttime");
        int year = yearBase + postTime.getYear();
        int month = postTime.getMonth()+1;
        fields.put("uuid", year + "!" + month + "!" + refid);
        fields.put("bid", rs.getInt("bid"));
        fields.put("year", year);
        fields.put("month", month);
        fields.put("postTime", rs.getTimestamp("posttime"));
        fields.put("title", rs.getString("title"));
        fields.put("content", rs.getString("contents"));

        String tags = rs.getString("taglist");
        if (tags != null && !tags.isEmpty()) {
            for (String tag : tags.split(" ")) {
                if (tag.startsWith("~")) {
                    int index = tag.lastIndexOf("~");
                    String label = tag.substring(1, index).toLowerCase();
                    String value = tag.substring(index + 1);
                    if (value != null && !"null".equals(value)) {
                    	if(label != null && label.equals("age")){
                    		fields.put("sys_" + label, Integer.parseInt(value));
                    	}else{
                    		fields.put("sys_" + label, value);
                    	}
                    }
                }
            }
        }
        return fields;
    }
	
	private void index(String hbindexName, BulkRequestBuilder bulkRequest, Map<String, Object> fields) throws IOException{
		String json = XContentFactory.jsonBuilder().map(fields).string();
		String bidStr = fields.get("bid")+"";
		String yearStr = fields.get("year")+"";
        bulkRequest.add( client.prepareIndex(hbindexName, typename, bidStr).setRouting(yearStr).setSource(json) );
	}
}
