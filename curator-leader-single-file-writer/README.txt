ZK / Curator Leader Election algorithm provides the ability to implement fault-tolerant replicated cluster of
services / processes in distributed env. Once current leader appears down, new leader will be elected. There are 2 appropriate ZK recipes:
1. Leader Latch, which is similar to CountDownLatch
2. Leader Election, which implements LeaderElection via ZK api.

Good example is here: https://github.com/apache/curator/blob/master/curator-examples/src/main/java/leader/LeaderSelectorExample.java

Compatibility notes:
Curator 2.x.x - compatible with both ZooKeeper 3.4.x and ZooKeeper 3.5.x
Curator 3.x.x - compatible only with ZooKeeper 3.5.x and includes support for new features such as dynamic reconfiguration, etc.

The implementation is very simplified & is aimed to show how fault-tolerance with ZK & Curator works
(e.g. LeaderSelector can accept an ExecutorService as param etc).

HOW IT WORKS:
1. Start ZK server on  localhost:2181
2. Start multiple FileWriterServer instances & obser ve just one would write to out.txt
3. Kill one of the started above FileWriterServer instances
4. After some delay ZK would Elect new Leader & another running instance would proceed writing to file
