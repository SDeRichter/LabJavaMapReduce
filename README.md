#TP MapReduce2 30/10

## 1.6 Send the JAR to the edge node
**1.6.3 Run the job**

````
-sh-4.2$ yarn jar hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar wordcount /user/gprecigout/davinci.txt test
20/10/30 16:21:32 INFO client.AHSProxy: Connecting to Application History server at hadoop-master03.efrei.online/163.172.100.24:10200
20/10/30 16:21:32 INFO hdfs.DFSClient: Created token for gprecigout: HDFS_DELEGATION_TOKEN owner=gprecigout@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1604071292744, maxDate=1604676092744, sequenceNumber=2982, masterKeyId=34 on ha-hdfs:efrei
20/10/30 16:21:32 INFO security.TokenCache: Got dt for hdfs://efrei; Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:efrei, Ident: (token for gprecigout: HDFS_DELEGATION_TOKEN owner=gprecigout@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1604071292744, maxDate=1604676092744, sequenceNumber=2982, masterKeyId=34)
20/10/30 16:21:32 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /user/gprecigout/.staging/job_1603290159664_0972
20/10/30 16:21:33 INFO input.FileInputFormat: Total input files to process : 1
20/10/30 16:21:33 INFO mapreduce.JobSubmitter: number of splits:1
20/10/30 16:21:34 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1603290159664_0972
20/10/30 16:21:34 INFO mapreduce.JobSubmitter: Executing with tokens: [Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:efrei, Ident: (token for gprecigout: HDFS_DELEGATION_TOKEN owner=gprecigout@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1604071292744, maxDate=1604676092744, sequenceNumber=2982, masterKeyId=34)]
20/10/30 16:21:34 INFO conf.Configuration: found resource resource-types.xml at file:/etc/hadoop/3.1.5.0-152/0/resource-types.xml
20/10/30 16:21:34 INFO impl.TimelineClientImpl: Timeline service address: hadoop-master03.efrei.online:8190
20/10/30 16:21:34 INFO impl.YarnClientImpl: Submitted application application_1603290159664_0972
20/10/30 16:21:34 INFO mapreduce.Job: The url to track the job: https://hadoop-master01.efrei.online:8090/proxy/application_1603290159664_0972/
20/10/30 16:21:34 INFO mapreduce.Job: Running job: job_1603290159664_0972
20/10/30 16:21:45 INFO mapreduce.Job: Job job_1603290159664_0972 running in uber mode : false
20/10/30 16:21:45 INFO mapreduce.Job:  map 0% reduce 0%
20/10/30 16:21:54 INFO mapreduce.Job:  map 100% reduce 0%
20/10/30 16:22:04 INFO mapreduce.Job:  map 100% reduce 100%
20/10/30 16:22:04 INFO mapreduce.Job: Job job_1603290159664_0972 completed successfully
20/10/30 16:22:04 INFO mapreduce.Job: Counters: 53
        File System Counters
                FILE: Number of bytes read=95666
                FILE: Number of bytes written=684529
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=163570
                HDFS: Number of bytes written=69045
                HDFS: Number of read operations=8
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
        Job Counters
                Launched map tasks=1
                Launched reduce tasks=1
                Other local map tasks=1
                Total time spent by all maps in occupied slots (ms)=21174
                Total time spent by all reduces in occupied slots (ms)=27072
                Total time spent by all map tasks (ms)=7058
                Total time spent by all reduce tasks (ms)=6768
                Total vcore-milliseconds taken by all map tasks=7058
                Total vcore-milliseconds taken by all reduce tasks=6768
                Total megabyte-milliseconds taken by all map tasks=10841088
                Total megabyte-milliseconds taken by all reduce tasks=13860864
        Map-Reduce Framework
                Map input records=2949
                Map output records=28661
                Map output bytes=274461
                Map output materialized bytes=95666
                Input split bytes=105
                Combine input records=28661
                Combine output records=6742
                Reduce input groups=6742
                Reduce shuffle bytes=95666
                Reduce input records=6742
                Reduce output records=6742
                Spilled Records=13484
                Shuffled Maps =1
                Failed Shuffles=0
                Merged Map outputs=1
                GC time elapsed (ms)=219
                CPU time spent (ms)=5960
                Physical memory (bytes) snapshot=1499574272
                Virtual memory (bytes) snapshot=7274795008
                Total committed heap usage (bytes)=1546649600
                Peak Map Physical memory (bytes)=1188941824
                Peak Map Virtual memory (bytes)=3397246976
                Peak Reduce Physical memory (bytes)=310632448
                Peak Reduce Virtual memory (bytes)=3877548032
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=163465
        File Output Format Counters
                Bytes Written=69045
-sh-4.2$ hdfs dfs -ls /user/gprecigout/test
Found 2 items
-rw-r--r--   3 gprecigout hdfs          0 2020-10-30 16:22 /user/gprecigout/test/_SUCCESS
-rw-r--r--   3 gprecigout hdfs      69045 2020-10-30 16:22 /user/gprecigout/test/part-r-00000
-sh-4.2$ hdfs dfs -head /user/gprecigout/test/part-r-00000
#43]    1
$5,000) 1
($1     1
(801)   1
(This   1
(a)     1
(after  1
(and    1
(any    1
(as     4
(available      1
-sh-4.2$ hdfs dfs -tail /user/gprecigout/test/part-r-00000
        2
“Poole,”        1
“Poor   1
“Project        5
“Pull   1
“Quite  2
“Rather 1
````

##1.8 Remarkable trees of Paris

````
-sh-4.2$ wget https://raw.githubusercontent.com/makayel/hadoop-examples-mapreduce/main/src/test/resources/data/trees.csv
--2020-10-30 17:03:04--  https://raw.githubusercontent.com/makayel/hadoop-examples-mapreduce/main/src/test/resources/data/trees.csv
Resolving raw.githubusercontent.com (raw.githubusercontent.com)... 151.101.120.133
Connecting to raw.githubusercontent.com (raw.githubusercontent.com)|151.101.120.133|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 16680 (16K) [text/plain]
Saving to: ‘trees.csv’

100%[==================================================================================================================================================>] 16,680      --.-K/s   in 0.001s

2020-10-30 17:03:04 (13.4 MB/s) - ‘trees.csv’ saved [16680/16680]

-sh-4.2$ hdfs dfs -copyFromLocal trees.csv
-sh-4.2$ hdfs dfs -ls
Found 13 items
drwx------   - gprecigout hdfs          0 2020-10-30 16:22 .staging
drwxr-xr-x   - gprecigout hdfs          0 2020-10-15 17:10 data
-rw-r--r--   1 gprecigout hdfs     163465 2020-10-15 01:20 davinci.txt
drwxr-xr-x   - gprecigout hdfs          0 2020-10-15 18:54 gutenberg
drwxr-xr-x   - gprecigout hdfs          0 2020-10-15 18:58 gutenberg-output
drwxr-xr-x   - gprecigout hdfs          0 2020-10-15 19:08 gutenberg-output.txt
drwxr-xr-x   - gprecigout hdfs          0 2020-10-15 19:05 gutenberg-output2
drwxr-xr-x   - gprecigout hdfs          0 2020-10-15 19:26 gutenberg-output5
drwxr-xr-x   - gprecigout hdfs          0 2020-10-06 10:10 raw
drwxr-xr-x   - gprecigout hdfs          0 2020-10-30 16:22 test
-rw-r--r--   3 gprecigout hdfs      16680 2020-10-30 17:08 trees.csv
drwxr-xr-x   - gprecigout hdfs          0 2020-10-15 19:18 user
drwxr-xr-x   - gprecigout hdfs          0 2020-10-15 01:23 wordcount
````
**1.8.1 Districts containing trees (very easy)**

````
-sh-4.2$ hdfs dfs -cat output_trees/part-r-00000
11
12
13
14
15
16
17
18
19
20
3
4
5
6
7
8
9
-sh-4.2$
````