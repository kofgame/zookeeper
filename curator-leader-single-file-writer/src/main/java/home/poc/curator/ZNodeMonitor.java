package home.poc.curator;

public interface ZNodeMonitor {

    /**
     * Sets appropriate canWrite flag to true, which is checked prior writing to file
     */
    void startWriting();

    /**
     * Sets the canWrite flag to false
     */
    void stopWriting();

    String getProcessName();
}