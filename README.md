Distributed-Key-Value
=====================

CS425 MP3 Distributed Key Value Project

javac *.java
rmic distributed.hash.table.DistributedHashTable
rmic distributed.hash.table.QueryRequest
rmic distributed.hash.table.InsertDeleteRequest
rmic distributed.hash.table.InsertDeleteReplicationRequest
rmic distributed.hash.table.ReplicationQueryRequest



java -Djava.security.policy=server.policy DHTServer -i 1 -c 4 -f serverSetting4.txt
java -Djava.security.policy=server.policy DHTServer -i 2 -c 4 -f serverSetting4.txt
java -Djava.security.policy=server.policy DHTServer -i 3 -c 4 -f serverSetting4.txt
java -Djava.security.policy=server.policy DHTServer -i 4 -c 4 -f serverSetting4.txt


java -Djava.security.policy=server.policy DHTAndFDServer -i 1 -c 4 -f serverSetting4.txt -d failureSetting4.txt -r debug
java -Djava.security.policy=server.policy DHTAndFDServer -i 2 -c 4 -f serverSetting4.txt -d failureSetting4.txt -r debug
java -Djava.security.policy=server.policy DHTAndFDServer -i 3 -c 4 -f serverSetting4.txt -d failureSetting4.txt -r debug
java -Djava.security.policy=server.policy DHTAndFDServer -i 4 -c 4 -f serverSetting4.txt -d failureSetting4.txt -r debug


java -Djava.security.policy=server.policy DHTAndFDServer -i 1 -c 6 -f serverSetting6.txt -d failureSetting6.txt -r debug
java -Djava.security.policy=server.policy DHTAndFDServer -i 2 -c 6 -f serverSetting6.txt -d failureSetting6.txt -r debug
java -Djava.security.policy=server.policy DHTAndFDServer -i 3 -c 6 -f serverSetting6.txt -d failureSetting6.txt -r debug
java -Djava.security.policy=server.policy DHTAndFDServer -i 4 -c 6 -f serverSetting6.txt -d failureSetting6.txt -r debug
java -Djava.security.policy=server.policy DHTAndFDServer -i 5 -c 6 -f serverSetting6.txt -d failureSetting6.txt -r debug
java -Djava.security.policy=server.policy DHTAndFDServer -i 6 -c 6 -f serverSetting6.txt -d failureSetting6.txt -r debug

running client for experiments

java DHTInteractiveClient -f clientSetting4.txt -r debug

clientSetting4.txt is the file that contains address of servers as bellow, In this scenario it is port number of processes  
15551,15552,15553,15554


By modifying bin/serverSeting4.txt we can add as many server we want. 
Also we can change the number of peer ids for each server and specify different number of keys for each server.
Format of serverSetting4.txt, this is an example of 4 servers, each connected to other 2 servers

Note: assumption is that the successors of each server are ordered 
server id,port,start key,size,address port,max Key number,address port,max Key number,...


There are two more scenarios as bellow:
1) serverSetting6.txt: 6 servers each one has 2 successors
	clientSetting6.txt is the client setting of this use case.

2) serverSetting8.txt: 8 servers each one has 2 successors and size of each server is variable
	clientSetting8.txt is the client setting of this use case
