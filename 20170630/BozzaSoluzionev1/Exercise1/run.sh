# Remove folders of the previous run
hdfs dfs -rm -r exam_ex1_data
hdfs dfs -rm -r exam_ex1_out

# Put input data collection into hdfs
hdfs dfs -put exam_ex1_data


# Run application
hadoop jar target/Exam2017_06_30_Exercise1-1.0.0.jar it.polito.bigdata.hadoop.exercise1.DriverBigData 2 exam_ex1_data/Prices.txt exam_ex1_out



