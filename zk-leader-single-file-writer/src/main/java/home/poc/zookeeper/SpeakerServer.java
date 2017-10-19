package home.poc.zookeeper;


import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *  Instantiates Speaker, ZNodeMonitor, adds Speaker as listener to ZNodeMonitor
 *  and starts whole thing up in separate fixed scheduled thread.
 *
 */
public class SpeakerServer {

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5);

    private static final int INTER_TASKS_DELAY_MILLIS = 300;
    public static final String CONFIG_FILE_PATH = "speaker.config";
    private ZNodeMonitor monitor;
    
    public static void main(String[] args) {
        String connectionString = readConfig(CONFIG_FILE_PATH);
        new SpeakerServer().start("a Speaker msg, being written to file", connectionString);
    }

    public void start(String msg, String connectionString) {
        Speaker speaker = null;
        try {
            speaker = new Speaker(msg);
        } catch (IOException e) {
            System.out.println("Couldn't read config file: " + e);
        }
        monitor = new ZNodeMonitor(connectionString);
        monitor.setListener(speaker);
        try {
            monitor.start();
        } catch (Exception e) {
            System.out.println("Unrecoverable error while attempting to start " + getClass().getSimpleName() + e);
            System.exit(1);
        }
        scheduler.scheduleWithFixedDelay(speaker, 0, INTER_TASKS_DELAY_MILLIS, TimeUnit.MILLISECONDS);
        System.out.println(getClass().getSimpleName() + " started & is gonna submit tasks with fixed delay " + INTER_TASKS_DELAY_MILLIS + " MILLIS");
    }

    private static String readConfig(String configFile) {
        Properties properties = new Properties();
        InputStream inputStream = ClassLoader.getSystemClassLoader ().getResourceAsStream (configFile);
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            System.out.println("Couldn't read config file: " + e);
            System.exit(1);
        }
        String connectionString = properties.getProperty("connectionString");
        System.out.println("Connection string: " + connectionString);
        IOUtils.closeQuietly(inputStream);
        return connectionString;
    }

}