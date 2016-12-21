package org.apache.zeppelin.spark;

import com.zetaris.lightning.sql.LightningConfigParser;
import com.zetaris.lightning.sql.zmpp.ZMPPConnection;
import org.apache.spark.sql.LightningSparkSession;
import org.apache.spark.sql.hive.LightningSQLEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

public class LightningSparkSessionBuilder {
    public static Logger logger = LoggerFactory.getLogger(LightningSparkSessionBuilder.class);

    private String lightningConfFilePath;
    private String metaStorePath;

    public LightningSparkSessionBuilder() {
    }

    public LightningSparkSessionBuilder withConfiguration(String lightningConfFilePath) {
        this.lightningConfFilePath = lightningConfFilePath;
        return this;
    }

    public LightningSparkSessionBuilder withMetaStore(String metaStorePath) {
        this.metaStorePath = metaStorePath;
        return this;
    }

    public LightningSparkSession build() throws IOException {
        if (lightningConfFilePath == null) {
            throw new RuntimeException("lightningConfFilePath is not set");
        }

        if (metaStorePath == null) {
            throw new RuntimeException("metaStorePath is not set");
        }

        addConfToClassPath();

        String lightningConfFile = lightningConfFilePath;
        int index = lightningConfFilePath.lastIndexOf(File.separator);
        if (index >= 0) {
            lightningConfFile = lightningConfFilePath.substring(index + 1);
        }

        logger.info("Loading lightning conf file : " + lightningConfFile);
        ZMPPConnection zc = LightningConfigParser.prepareZMPPConnection(lightningConfFile);
        LightningSQLEnv.init(zc);

        return LightningSQLEnv.sparkSession();
    }

    private void addConfToClassPath() throws IOException {
        String confDir = lightningConfFilePath;
        int index = lightningConfFilePath.lastIndexOf(File.separator);
        if (index >= 0) {
            confDir = lightningConfFilePath.substring(0, index + 1);
            logger.info("Adding conf dir for lightning conf file : " + confDir);
            File confDirPath = new File(confDir);
            addFileToClassPath(confDirPath);
        }

        index = metaStorePath.lastIndexOf(File.separator);
        if (index >= 0) {
            confDir = metaStorePath.substring(0, index + 1);
            logger.info("Adding conf dir for lightning conf file : " + confDir);
            File confDirPath = new File(confDir);
            addFileToClassPath(confDirPath);
        }

    }

    private void addFileToClassPath(File file) throws IOException {
        final Class[] parameters = new Class[]{URL.class};

        URLClassLoader sysloader = (URLClassLoader) ClassLoader.getSystemClassLoader();
        Class sysclass = URLClassLoader.class;

        try {
            Method method = sysclass.getDeclaredMethod("addURL", parameters);
            method.setAccessible(true);
            method.invoke(sysloader, new Object[]{file.toURI().toURL()});
        } catch (Throwable t) {
            t.printStackTrace();
            throw new IOException("Error, could not add file to system classloader");
        }
    }
}
