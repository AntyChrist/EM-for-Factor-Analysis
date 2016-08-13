#!/bin/sh -ex

nIters=3

cd $HADOOP_PREFIX/Workspace/MapReduce/matlab
mkdir -p input
./split_file.pl fa_data_2.txt 8

$HADOOP_PREFIX/bin/hdfs dfs -rm -r -f /user/liusining/stats; 
$HADOOP_PREFIX/bin/hdfs dfs -mkdir -p /user/liusining/stats/input
$HADOOP_PREFIX/bin/hdfs dfs -put $HADOOP_PREFIX/Workspace/MapReduce/matlab/input /user/liusining/stats
$HADOOP_PREFIX/bin/hdfs dfs -put $HADOOP_PREFIX/Workspace/MapReduce/matlab/gmm.txt /user/liusining/stats

sleep 1

for i in `seq $nIters`
do
 	echo "Iteration $i"
	$HADOOP_PREFIX/bin/hdfs dfs -rm -r -f /user/liusining/stats/output; 
	sleep 1;
	cd $HADOOP_PREFIX;
	time $HADOOP_PREFIX/bin/hadoop jar ./Workspace/fa8.jar parallel.fa.MapRedFA /user/liusining/stats/input /user/liusining/stats/output

done

#view the output by  ./bin/hdfs dfs -cat /user/liusining/stats/fa.txt


