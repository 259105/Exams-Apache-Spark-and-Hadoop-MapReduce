# Remove folders of the previous run
hdfs dfs -rm -r exam_ex2_data

hdfs dfs -rm -r exam_ex2A_out
hdfs dfs -rm -r exam_ex2B_out

# Put input data collection into hdfs
hdfs dfs -put exam_ex2_data

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise2.SparkDriver --deploy-mode cluster --master yarn target/Exam2016_07_01_Exercise2-1.0.0.jar "exam_ex2_data/Books.txt" "exam_ex2_data/BoughtBooks.txt" 10 exam_ex2A_out exam_ex2B_out

