The pet (POC) project demonstrates ZK Leader Election algorithm & is aimed for launching under Windows, and is very simplified

If you need Leader Election in commercial project, use Curator, it provides much higher level API & is much easier to use.

1. Launch ZK server:
dist-zk-3.4.9\bin\zkServer.cmd (it relies on customized zoo_2180.cfg)
2. Start 3 instances of SpeakerServer
3. Observe at any moment just one running SpeakerServer would be 
writing to out.txt file, the rest woul wait.
4. Kill one of the running SpeakerServer-s and observe the other instance 
would start writing to file

Good article to re-fresh basics upon ZK is https://www.tutorialspoint.com/zookeeper/zookeeper_quick_guide.htm