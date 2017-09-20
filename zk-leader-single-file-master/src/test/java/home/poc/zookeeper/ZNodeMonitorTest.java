package home.poc.zookeeper;

import junit.framework.Assert;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ZNodeMonitorTest {

    @InjectMocks
    ZNodeMonitor underTest = new ZNodeMonitor("localhost:2180");

    @Mock
    private ZooKeeper zooKeeper;
    @Mock
    private ZNodeMonitorListener listener;

    @Before
    public void setUp() {
        when(listener.getProcessName()).thenReturn("Speaker-pid-1276");
    }

    @Test
    public void testParseSequenceNumber() throws Exception {
        assertEquals(44920000000026L, underTest.parseSequenceNumber("/ELECTION/Speaker-pid-44920000000026"));
    }

    @Test
    public void testGetLowestNumber() throws IOException, InterruptedException, KeeperException {
        underTest.setSequenceNumber(50);
        List<String> children = new ArrayList<String>();
        children.add("/ELECTION/Speaker-pid-2408-0000000025");
        children.add("/ELECTION/Speaker-pid-4492-0000000026");
        children.add("/ELECTION/Speaker-pid-1233-0000000333");
        children.add("/ELECTION/Speaker-pid-003-000033");
        long expected = 25;
        assertEquals(expected, underTest.getLowestNumber(children));
    }

    @Test
    public void testProcessWatchedEvent() throws InterruptedException, KeeperException {
        WatchedEvent watchedEvent = new WatchedEvent(
                Watcher.Event.EventType.None,
                Watcher.Event.KeeperState.SyncConnected,
                "testPath");

        when(zooKeeper.exists(anyString(), anyBoolean())).thenReturn(null);
        when(zooKeeper.create(anyString(), any(byte[].class), Mockito.anyListOf(ACL.class), Mockito.any(CreateMode.class)))
            .thenReturn("/ELECTION/Speaker-pid-1276-0000000028");

        underTest.process(watchedEvent);

        Mockito.verify(zooKeeper, Mockito.times(1)).create(
                "/ELECTION", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Assert.assertEquals(28, underTest.getSequenceNumber());

    }
}