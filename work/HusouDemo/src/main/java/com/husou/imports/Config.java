package com.husou.imports;

import java.io.IOException;
import java.util.Properties;

public class Config {
    private String dbUrl;
    private String dbUser;
    private String dbPass;
    private int batchSize;
    private int waitInterval;
    private boolean keepRunning;
    private String clusterName;
    private String esHost;
    private String indexName;
    private String typeName;

    public static Config instance = new Config();
    public static Properties prop;
    
    private Config() {
    	try {
    		prop = new Properties();
			prop.load(Config.class.getClassLoader().getResourceAsStream("config.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
    	init();
    }

    public void init() throws IllegalArgumentException {
        this.dbUrl = prop.getProperty("dbUrl");
        this.dbUser = prop.getProperty("dbUser");
        this.dbPass = prop.getProperty("dbPass");
        String batchSize = prop.getProperty("batchSize", "1000");
        this.batchSize = Integer.valueOf(batchSize);
        String waitInterval = prop.getProperty("waitInterval", "300");
        this.waitInterval = Integer.valueOf(waitInterval);
        String keepRunning = prop.getProperty("keepRunning", "false");
        this.keepRunning = Boolean.valueOf(keepRunning);
        this.clusterName = prop.getProperty("es.cluster.name", "elasticsearch");
        this.esHost = prop.getProperty("esHost", "localhost");
        this.indexName = prop.getProperty("indexName", "unicom");
        this.typeName = prop.getProperty("typeName", "searchword");
    }

    public String getProperty(String key){
    	return prop.getProperty(key);
    }
    
    public String getDbUrl() {
        return dbUrl;
    }

    public void setDbUrl(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    public String getDbUser() {
        return dbUser;
    }

    public void setDbUser(String dbUser) {
        this.dbUser = dbUser;
    }

    public String getDbPass() {
        return dbPass;
    }

    public void setDbPass(String dbPass) {
        this.dbPass = dbPass;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getWaitInterval() {
        return waitInterval;
    }

    public void setWaitInterval(int waitInterval) {
        this.waitInterval = waitInterval;
    }

    public boolean isKeepRunning() {
        return keepRunning;
    }

    public void setKeepRunning(boolean keepRunning) {
        this.keepRunning = keepRunning;
    }

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getEsHost() {
		return esHost;
	}

	public void setEsHost(String esHost) {
		this.esHost = esHost;
	}

	public String getIndexName() {
		return indexName;
	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	public String getTypeName() {
		return typeName;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}
}
