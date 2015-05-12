package com.husou.imports;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

public class BeatConverter {
	private static Logger log = Logger.getLogger(BeatConverter.class);

    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        InputStream file = null;
        try {
            prop.load(BeatConverter.class.getClassLoader().getResourceAsStream("config.properties"));
            
            for (String arg : args) {
                String[] nv = arg.split("=");
                prop.put(nv[0], nv[1]);
            }

            Config config = new Config(prop);
            Thread importer = new ESImport(config);
            importer.start();

        } catch (IOException ex) {
            ex.printStackTrace();
            log.error(ex);
        } finally {
            if (file != null) {
                try {
                    file.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                    log.error(ex);
                }
            }
        }
    }
}
