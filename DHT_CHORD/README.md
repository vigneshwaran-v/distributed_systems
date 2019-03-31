# Distributed Hash Table based on CHORD

This is a simple Distributed Hash Table designed based on CHORD protocol which supports inserts,deletes and query operations. The following features were implemented in this project:

	1. ID space partitioning and re-partitioning
	
	2. Ring-based routing
	
	3. Handled node joins to the Chord ring.
	
	
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
.\simpledht-grading.exe <apk path>
```		
Provide the appropriate apk file path as the command line argument.
	 
	 
## Grading script tests the following:

	1) Local insert/query/delete operations work on a DHT containing a single AVD.
	
	2) The insert operation works correctly with static membership of 5 AVDs.
	
	3) The query operation works correctly with static membership of 5 AVDs.
	
	4) The insert operation works correctly with between 1 and 5 AVDs (and possibly changing membership).
	
	5) The query operation works correctly with between 1 and 5 AVDs (and possibly changing membership).
	
	6) The delete operation works correctly with between 1 and 5 AVDs (and possibly changing membership).
	 

	
	
	
	

