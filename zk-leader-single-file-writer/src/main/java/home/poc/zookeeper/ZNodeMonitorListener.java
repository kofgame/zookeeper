package home.poc.zookeeper;

public interface ZNodeMonitorListener {

    void startSpeaking();
    void stopSpeaking();

    String getProcessName();
}