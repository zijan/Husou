package com.husou.imports;

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

    public Config() {
    }

    public Config(Properties prop) throws IllegalArgumentException {
    	
        this.dbUrl = prop.getProperty("dbUrl");
        this.dbUser = prop.getProperty("dbUser");
        this.dbPass = prop.getProperty("dbPass");
        String batchSize = prop.getProperty("batchSize", "10");
        this.batchSize = Integer.valueOf(batchSize);
        String waitInterval = prop.getProperty("waitInterval", "10");
        this.waitInterval = Integer.valueOf(waitInterval);
        String keepRunning = prop.getProperty("keepRunning", "false");
        this.keepRunning = Boolean.valueOf(keepRunning);
        this.clusterName = prop.getProperty("es.cluster.name", "elasticsearch");
        this.esHost = prop.getProperty("esHost", "localhost");
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
}
