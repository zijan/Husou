package com.husou.imports;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
import org.elasticsearch.client.transport.NoNodeAvailableException;
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
    
    private String indexName = "unicom";
    private String typeName = "searchword";
    
	public ESImport(Config config) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        conn = DriverManager.getConnection(config.getDbUrl(), config.getDbUser(), config.getDbPass());
        this.config = config;
        
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", config.getClusterName()).build();
        client = new TransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(config.getEsHost(), 9300));
        
        indexName = config.getIndexName();
        typeName = config.getTypeName();
    }
	
	public void run(){
        logger.info("started thread batch size:" + config.getBatchSize());
        String sql = "SELECT CONCAT(t.Number, UNIX_TIMESTAMP(t.CreateTime)) rid, t.* FROM `2015` t WHERE UNIX_TIMESTAMP(t.CreateTime) >= ? ORDER BY t.CreateTime LIMIT " + config.getBatchSize();

        PreparedStatement stat = null;
        ResultSet rs = null;
        
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        
        long storedCreateTimeLong = loadLastCreateTime();
        if(storedCreateTimeLong == -1){
        	logger.info("error on elastic search, exit!");
        	return;
        }

        try {
            while (true) {
                try {

                	long timeflag = System.currentTimeMillis();
                	
                    stat = conn.prepareStatement(sql);
                    stat.setLong(1, storedCreateTimeLong);
                    rs = stat.executeQuery();

                    long storedCreateTimeLongLast = 0;
                    boolean empty = true;
                    int i = 0;
                    while (rs.next()) {
                        i++;
                        Map<String, Object> fields = rowToMap(rs);
                        index(indexName, bulkRequest, fields);
                        storedCreateTimeLongLast = rs.getTimestamp("CreateTime").getTime() / 1000;
                    }
                    if(storedCreateTimeLongLast > storedCreateTimeLong ){
                    	empty = false;
                    	storedCreateTimeLong = storedCreateTimeLongLast;
                    }
                    
                    if(!empty){
                    	BulkResponse bulkResponse = bulkRequest.execute().actionGet(); 
                        bulkRequest = client.prepareBulk();
            	        if (bulkResponse.hasFailures()) { 
            	            logger.debug(bulkResponse.buildFailureMessage());
            	        }
            	        
            	        long time = (System.currentTimeMillis() - timeflag)/1000;
                        logger.info("added " + i + " docs [took: "+time+"s] last create time [" +storedCreateTimeLong + "." + new Date(storedCreateTimeLong * 1000)+ "]");
                        if (!config.isKeepRunning()) {
                            logger.info("Not in keep running mode. job finished");
                            break;
                        }
                    } else {
                        int waitInterval = config.getWaitInterval();
                        logger.info("wait " + waitInterval + " seconds for next fetch");
                        Thread.sleep(1000 * waitInterval);
                    }

                } catch (Exception ex) {
                    ex.printStackTrace();
                    logger.error("Exception: ", ex);
                    continue;
                } finally {
                    logger.info("completed one batch");
                }
            }
        } finally {
            logger.info("************** completed thread *******************");
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
        Date createTime = rs.getTimestamp("CreateTime");
        int year = yearBase + createTime.getYear();
        int month = createTime.getMonth()+1;
        fields.put("rid", rs.getString("rid"));
        fields.put("year", year);
        fields.put("month", month);
        fields.put("createTime", createTime);
        fields.put("area", rs.getString("Area"));
        fields.put("searchString", rs.getString("SearchString"));
        return fields;
    }
	
	private void index(String indexName, BulkRequestBuilder bulkRequest, Map<String, Object> fields) throws IOException{
		String json = XContentFactory.jsonBuilder().map(fields).string();
		String rid = (String)fields.get("rid");
		String yearmonthStr = ""+fields.get("year")+fields.get("month");
        bulkRequest.add( client.prepareIndex(indexName, typeName, rid).setRouting(yearmonthStr).setSource(json) );
	}
	
	private long loadLastCreateTime(){
		long storedCreateTimeLong = 0;

		try {
			//check index exist
			client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet(); 
			ClusterStateResponse response = client.admin().cluster().prepareState().execute().actionGet(); 
			boolean hasIndex = response.getState().metaData().hasIndex(indexName);
			if(hasIndex){
				//find last createTime
				SearchRequestBuilder builder= client.prepareSearch(indexName)  
			            .setTypes(typeName)  
			            .setSearchType(SearchType.DEFAULT)  
			            .setFrom(0)  
			            .setSize(1)
			            .addSort(SortBuilders.fieldSort("createTime").order(SortOrder.DESC)); 

			    SearchResponse searchResponse = builder.execute().actionGet();
			    SearchHits hits = searchResponse.getHits();
			    
			    if(hits.totalHits() < 1){
			    	storedCreateTimeLong = 0;
			    }else{
			    	try {
			    		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.000Z");
			            Date createTime = df.parse(((String)hits.getAt(0).getSource().get("createTime")).replace("Z", "GMT"));
			        	storedCreateTimeLong = createTime.getTime() / 1000;
					} catch (ParseException e) {
						e.printStackTrace();
					}
			    }
			}else{
				storedCreateTimeLong = -1;
			}
		} catch (NoNodeAvailableException e) {
			storedCreateTimeLong = -1;
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
        return storedCreateTimeLong;
	}
}
