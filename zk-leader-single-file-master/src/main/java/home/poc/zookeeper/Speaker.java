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

package home.poc.zookeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.management.ManagementFactory;

/**
 * Writes it’s PID (processId) and incremental counter to file (out.txt)
 */
public class Speaker implements Runnable, ZNodeMonitorListener {

    public static final String ID_DELIMITER = "-";

    private final static Logger logger = LoggerFactory.getLogger(Speaker.class);
    private BufferedWriter writer = new BufferedWriter(new FileWriter(new File("out.txt"), true));

    private String message;
    private String processName;
    private long counter = 0;
    private volatile boolean canSpeak = false;

    public Speaker(String message) throws IOException {
        this.message = message;
        this.processName = getUniqueIdentifier();
    }

    private static String getUniqueIdentifier() {
        String processName = ManagementFactory.getRuntimeMXBean().getName();
        String processId = processName.substring(0, processName.indexOf("@"));
        return "Speaker" + ID_DELIMITER + "pid" + ID_DELIMITER + processId;
    }

    public void run() {
        try {
            if (canSpeak) {
                handleTask();
            }
        } catch (IOException e) {
            System.out.println(e.getLocalizedMessage());
            System.exit(1);
        }
    }

    public void handleTask() throws IOException {
        String msg = message + ": " + counter++ + " " + processName;
        writer.append(msg + "\n");
        writer.flush();     // append data to file line by line
        System.out.println(msg);
    }

    @Override
    public void startSpeaking() {
        this.canSpeak = true;
    }
    @Override
    public void stopSpeaking() {
        this.canSpeak = false;
    }

    @Override
    public String getProcessName() {
        return processName;
    }

}