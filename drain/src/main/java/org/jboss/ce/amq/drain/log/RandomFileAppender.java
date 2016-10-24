package org.jboss.ce.amq.drain.log;

import java.security.SecureRandom;
import java.util.Random;

import org.apache.log4j.FileAppender;
import org.jboss.ce.amq.drain.Utils;

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public class RandomFileAppender extends FileAppender {
    private static final Random RANDOM = new SecureRandom();

    private String baseDir = Utils.getSystemPropertyOrEnvVar("log.file.dir", "/opt/amq/data/");

    @Override
    public void activateOptions() {
        setFile(getBaseDir() + String.format("drainlog-%s.log", RANDOM.nextInt(1000)));
        super.activateOptions();
    }

    public String getBaseDir() {
        return baseDir;
    }

    public void setBaseDir(String baseDir) {
        this.baseDir = baseDir;
    }
}
