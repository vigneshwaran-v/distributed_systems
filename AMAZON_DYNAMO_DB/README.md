# Dynamo DB

This is a distributed key-value storage system designed based on a simplified version of Amazon Dynamo DB.

## Implementation Specifics

  **1. Membership**

  Just as the original Dynamo, every node can know every other node. This means that each node knows all other nodes in the system and also knows exactly which partition belongs to which node; any node can forward a request to the correct node without using a ring-based routing.

  **2. Request routing**
  
  Unlike Chord, each Dynamo node knows all other nodes in the system and also knows exactly which partition belongs to which node. Under no failures, a request for a key is directly forwarded to the coordinator (i.e., the successor of the key), and the coordinator should be in charge of serving read/write operations.
  
  **3. Chain replication**
    
  Linearizability is achieved using chain replication. [Reference](http://www.cs.cornell.edu/home/rvr/papers/osdi04.pdf)

  In chain replication, a write operation always comes to the first partition; then it propagates to the next two partitions in sequence. The last partition returns the result of the write. A read operation always comes to the last partition and reads the value from the last partition.

  **4. Failure handling**
  
  Just as the original Dynamo, each request can be used to detect a node failure. For this purpose, we use a timeout for a socket read; and if a node does not respond within the timeout, we consider it a failure.

  Relying on socket creation or connect status to determine if a node has failed is not a foolproof startegy. Due to the Android emulator networking setup, it is not safe to rely on socket creation or connect status to judge node failures. 
  
  When a coordinator for a request fails and it does not respond to the request, its successor can be contacted next for the request


## Steps to the test the application:

1. Run the following commands to setup the AVDs,
	
```
python create_avd.py
python updateavd.py
```
2. To start the AVDs use,
```
python run_avd.py 5
```
3. To setup the emulator network use,
```
python set_redir.py 10000
```		
4. Run the grader using the command,
```
.\simpledynamo-grading.exe <apk path>
```		
Provide the appropriate apk file path as the command line argument.

5. Use ‘-p’ argument, to specify which testing phase you want to test.

6) use '-h' to see what other options are available.


## Grading script tests the following:

1. Testing basic operations
• This phase will test insert, query, and delete (including the special keys @ and *). This will ensure that all data is correctly replicated. It will not perform any concurrent operations or test failures

2. Testing concurrent operations with differing keys
• This phase will test your implementation under concurrent operations, but without failure.
• The tester will use different key-value pairs inserted and queried concurrently on various nodes.
	
3. Testing concurrent operations with like keys
• This phase will test your implementation under concurrent operations with the same keys, but no failure.
• The tester will use the same set of key-value pairs inserted and queried concurrently on all nodes.
	
4. Testing one failure
• This phase will test every operation under a single failure.
• For each operation, one node will crash before the operation starts. After the operation is done, the node will recover.
• This will be repeated for each operation.
	
5. Testing concurrent operations with one failure
• This phase will execute various operations concurrently and crash one node in the middle of execution. After some time, the node will recover (also during execution).

6. Testing concurrent operations with repeated failures
• This phase will crash one node at a time, repeatedly. That is, one node will crash and then recover during execution, and then after its recovery another node will crash and then recover, etc.
• There will be a brief period of time between each crash-recover sequence.
