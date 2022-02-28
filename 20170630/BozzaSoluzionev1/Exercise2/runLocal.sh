# Remove folders of the previous run
rm -rf exam_ex2A_out
rm -rf exam_ex2B_out



NW="2"

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise2.SparkDriver --deploy-mode client --master local target/Exam2017_06_30_Exercise2_v2-1.0.0.jar "exam_ex2_data/Prices.txt" ${NW} exam_ex2A_out exam_ex2B_out
