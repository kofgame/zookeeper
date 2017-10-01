/*
 * Copyright (c) 2011, Olivier Van Acker.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package home.poc.curator;

import org.apache.commons.io.IOUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Writes it’s PID (processId) and incremental processCounter to file (out.txt)
 */
public class FileWriterClient extends LeaderSelectorListenerAdapter
        implements ZNodeMonitor, Runnable, Closeable{

    public static final String ID_DELIMITER = "-";
    public static final String LEADER_PATH = "/LEADER_PATH";

    private final static Logger logger = LoggerFactory.getLogger(FileWriterClient.class);
    private BufferedWriter writer = new BufferedWriter(new FileWriter(new File("out.txt"), true));

    private String beingWrittenToFileMsg;
    private long processCounter = 0;

    // fields from Curator example
    private final String processName = getPidUniqueIdentifier();
    private final LeaderSelector leaderSelector;
    private final AtomicInteger leaderCount = new AtomicInteger();

    private volatile boolean canWrite = false;

    /**
     * Initializes this instance & LeaderSelector
     * @param curatorClient curatorFramework client
     * @param leaderPath leader path
     * @throws IOException
     */
    public FileWriterClient(String beingWrittenToFileMsg, CuratorFramework curatorClient, String leaderPath) throws IOException {
        this.beingWrittenToFileMsg = beingWrittenToFileMsg;
        // also can pass ExecutorService to below leaderSelector
        leaderSelector = new LeaderSelector(curatorClient, leaderPath, this);
        // When takeLeadership() returns, this instance isn't re-queued. Hence put LeaderSelector into mode, in which it will always re-queue itself
        leaderSelector.autoRequeue();
    }

    @Override
    public void takeLeadership(CuratorFramework curatorFramework) {
        // having obtained LEADERSHIP, do the work until loosing leadership or interruption
        System.out.println(processName + " is now the leader. Starting to write in file...\n");
        System.out.println(processName + " has been leader " + leaderCount.getAndIncrement() + " time(s) before\n");
        startWriting();
    }

    @Override
    public void run() {
        appendDataToFileInLoop();
    }

    private void appendDataToFileInLoop() {
        while (true) {
            if (canWrite) {
                try {
                    appendDataToFile();
                    emulateDelayToPreventTooQuickFileGrowth();
                } catch (IOException e) {
                    System.out.println(e.getLocalizedMessage());
                    System.exit(1);
                }
            }
        }
    }

    /**
     * Emulate delay for to avoid too quick file size growth
     * @throws InterruptedException
     */
    private void emulateDelayToPreventTooQuickFileGrowth(){
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            System.err.println(e.getLocalizedMessage());
        }
    }

    public void start() throws IOException {
        // Start participation in Election, mandatory in new thread
        leaderSelector.start();
    }
    @Override
    public void close() throws IOException {
        stopWriting();
        leaderSelector.close();
        IOUtils.closeQuietly(writer);
    }

    public void appendDataToFile() throws IOException {
        String msg = beingWrittenToFileMsg + ": " + processCounter++ + " " + processName;
        writer.append(msg + "\n");
        writer.flush();     // append data to file line by line
        System.out.println(msg);
    }

    private String getPidUniqueIdentifier() {
        String processName = ManagementFactory.getRuntimeMXBean().getName();
        String processId = processName.substring(0, processName.indexOf("@"));
        return "FileWriterClient" + ID_DELIMITER + "pid" + ID_DELIMITER + processId;
    }

    @Override
    public void startWriting() {
        this.canWrite = true;
    }
    @Override
    public void stopWriting() {
        this.canWrite = false;
    }
    @Override
    public String getProcessName() {
        return processName;
    }

}