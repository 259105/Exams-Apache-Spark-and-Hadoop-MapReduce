# Remove folders of the previous run
hdfs dfs -rm -r exam_ex1_data
hdfs dfs -rm -r exam_ex1_out

# Put input data collection into hdfs
hdfs dfs -put exam_ex1_data


# Run application
hadoop jar target/Exam2019_07_02_Exercise1-1.0.0.jar it.polito.bigdata.hadoop.exercise1.DriverBigData 2 exam_ex1_data/Patches.txt exam_ex1_out



