# Remove folders of the previous run
rm -rf exam_ex2A_out
rm -rf exam_ex2B_out


# Run application
spark-submit  --class it.polito.bigdata.spark.exercise2.SparkDriver --deploy-mode client --master local target/Exam2018_01_22_Exercise2-1.0.0.jar "exam_ex2_data/Books.txt" "exam_ex2_data/Purchases.txt" exam_ex2A_out exam_ex2B_out
