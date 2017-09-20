/*
 * TODO: add javadoc
 */

package home.poc.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Talks asynchronously to ZK server and depending on response and state of connection
 * decides if Speaker has to start or stop writing to output file
 */
public class ZNodeMonitor implements Watcher, AsyncCallback.ChildrenCallback {

    final Logger logger = LoggerFactory.getLogger(ZNodeMonitor.class);
    private final String ROOT = "/ELECTION";
    private int SESSION_TIMEOUT = 5000;

    private final String PID_ZNODE_DELIMITER = "-";

    private ZNodeMonitorListener listener;
    private ZooKeeper zooKeeper;

    private long sequenceNumber;
    private String connectionString;
    private String znode;

    public ZNodeMonitor(String connectionString) {
        this.connectionString = connectionString;
    }

    // just for tests
    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    public void setListener(ZNodeMonitorListener listener) {
        this.listener = listener;
        this.znode = ROOT + "/" + listener.getProcessName() + PID_ZNODE_DELIMITER;
    }

    public void start() throws IOException {
        this.zooKeeper = new ZooKeeper(connectionString, SESSION_TIMEOUT, this);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println("Received " + watchedEvent.getType() + " EVENT from ZK server");
        switch (watchedEvent.getType()) {
            case None:
                processNoneEvent(watchedEvent);
                break;
            case NodeCreated:
            case NodeDeleted:
                listener.stopSpeaking();
                createZnode();
            case NodeDataChanged:
            case NodeChildrenChanged:
            default:
                String path = watchedEvent.getPath();
                if (path != null && path.equals(ROOT)) {
                    System.out.println(listener.getProcessName() + " -> something has changed on root node");
                    zooKeeper.getChildren(ROOT, true, this, null);
                }
                break;
        }
        try {
            zooKeeper.exists(ROOT, this);
        } catch (Exception e) {
            shutdown(e);
        }
    }

    /**
     * Something changed related to ZK connection or session
     * @param event
     */
    private void processNoneEvent(WatchedEvent event) {
        switch (event.getState()) {
            case SyncConnected:
                System.out.println(listener.getProcessName() + " is connected to Zookeeper");
                createRootIfNotExists();
                System.out.println(listener.getProcessName() + ": putting watch on " + ROOT);
                //set watch
                zooKeeper.getChildren(ROOT, true, this, null);
                sequenceNumber = createZnode();
                System.out.println("<---- Initialized sequenceNumber: " + sequenceNumber + " ---->");
                break;
            case Disconnected:
                System.out.println("Disconnected event");
                listener.stopSpeaking();
                break;
            case Expired:
                System.out.println("Expired event");
                listener.stopSpeaking();
                break;
            default:
                break;
        }
    }

    private void createRootIfNotExists() {
        Stat stat = null;
        try {
            stat = zooKeeper.exists(ROOT, true);
            if (stat == null) {
                zooKeeper.create(ROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException | InterruptedException ex) {
            System.err.println("Exception during create createRootIfNotExists: " + ex.getLocalizedMessage());
        }
    }

    /**
     * Sequential znodes guaranty that znode path will be unique.
     * @return
     */
    private long createZnode() {
        try {
            znode = zooKeeper.create(znode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println("<---- Created znode: " + znode + " ----->");
        } catch (KeeperException | InterruptedException ex) {
            System.err.println("Exception during create createZnode: " + ex.getLocalizedMessage());
        }
        return parseSequenceNumber(znode);
    }

    public long parseSequenceNumber(String znode) {
        return Long.parseLong( znode.substring(znode.lastIndexOf(PID_ZNODE_DELIMITER) + 1) );
    }

    /**
     * This callback is used to retrieve <b>children of the Node</b>.
     * @param rc            return code or result of the call.
     * @param path          the path that we passed to asynchronous calls.
     * @param ctx           whatever context object that we passed to asynchronous calls.
     * @param nodeChildren  unordered array of children of the node on given path.
     */
    @Override
    public void processResult(int rc, String path, Object ctx, List<String> nodeChildren) {
        System.out.println("Node Children: " + nodeChildren.toString() + " for path: " + path);
        switch (rc) {
            case KeeperException.Code.Ok:
                if(getLowestNumber(nodeChildren) == sequenceNumber) {
                    listener.startSpeaking();
                }
                else {
                    listener.stopSpeaking();
                }
                break;
            default:
                listener.stopSpeaking();
                break;
        }
    }

    long getLowestNumber(List<String> children) {
        long lowest = sequenceNumber;
        for (String child : children) {
            long current = parseSequenceNumber(child);
            if(current < lowest) {
                lowest = current;
            }
        }
        return lowest;
    }

    public void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public void shutdown(Exception e) {
        System.err.println("Unrecoverable error while trying to set a watch on election znode, shutting down client: " + e);
        System.exit(1); //useless to proceed
    }

}