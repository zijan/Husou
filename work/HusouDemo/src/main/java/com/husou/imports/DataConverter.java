package com.husou.imports;

import java.io.IOException;

import org.apache.log4j.Logger;

public class DataConverter {
	private static Logger log = Logger.getLogger(DataConverter.class);

    public static void main(String[] args) throws Exception {
        try {
            Thread importer = new ESImport(Config.instance);
            importer.start();
        } catch (IOException ex) {
            ex.printStackTrace();
            log.error(ex);
        } 
    }
}
