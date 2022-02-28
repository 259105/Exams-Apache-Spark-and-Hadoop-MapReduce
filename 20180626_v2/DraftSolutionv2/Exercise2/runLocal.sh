# Remove folders of the previous run
rm -rf exam_ex2A_out
rm -rf exam_ex2B_out



PrecThr="11.0"
WindThr="1.5"

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise2.SparkDriver --deploy-mode client --master local target/Exam2018_06_26_v2_Exercise2-1.0.0.jar ${PrecThr} ${WindThr} "exam_ex2_data/ClimateData.txt" exam_ex2A_out exam_ex2B_out
