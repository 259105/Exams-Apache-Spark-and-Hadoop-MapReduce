# Remove folders of the previous run
rm -rf exam_ex2A_out
rm -rf exam_ex2B_out



CPUthr="10.0"
RAMthr="1.5"

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise2.SparkDriver --deploy-mode client --master local target/Exam2018_06_26_Exercise2-1.0.0.jar ${CPUthr} ${RAMthr} "exam_ex2_data/PerformanceLog.txt" exam_ex2A_out exam_ex2B_out
