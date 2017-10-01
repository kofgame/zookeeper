package home.poc.curator;


import org.apache.commons.io.IOUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *  Instantiates FileWriterClient, and starts whole thing up in separate fixed scheduled thread.
 *
 */
public class FileWriterServer {

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5);

    private static final int INTER_TASKS_DELAY_MILLIS = 500;
    public static final String CONFIG_FILE_PATH = "fileWriter.config";

    public static void main(String[] args) {
        String connectionString = readConfig(CONFIG_FILE_PATH);
        new FileWriterServer().start("a msg, to write in file", connectionString);
    }

    public void start(String beingWrittenToFileMsg, String connectionString) {
        FileWriterClient fileWriterListener = null;
        // These are reasonable args for the ExponentialBackoffRetry: 1-st retry will wait 1 sec, 2-d - up to 2 seconds, 3-rd - up to 4 seconds
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        // Simplest way to construct CuratorFramework instance (this implies default values will be used)
        CuratorFramework curatorFrameworkClient = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        try {
            fileWriterListener = new FileWriterClient(beingWrittenToFileMsg, curatorFrameworkClient, FileWriterClient.LEADER_PATH);
        } catch (IOException e) {
            System.out.println("Couldn't read config file: " + e);
        }
        try {
            curatorFrameworkClient.start();
            fileWriterListener.start();
        } catch (Exception e) {
            System.out.println("Unrecoverable error while attempting to start " + getClass().getSimpleName() + e);
            System.exit(1);
        }
        scheduler.scheduleWithFixedDelay(fileWriterListener, 0, INTER_TASKS_DELAY_MILLIS, TimeUnit.MILLISECONDS);
        System.out.println("<-- " + getClass().getSimpleName() + " started & is gonna submit tasks with fixed delay " + INTER_TASKS_DELAY_MILLIS + " MILLIS -->");
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