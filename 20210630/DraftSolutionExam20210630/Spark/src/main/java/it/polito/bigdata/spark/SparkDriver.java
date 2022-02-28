package it.polito.bigdata.spark;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class SparkDriver {

    public static void main(String[] args) {
        String salesPath, motorbikesPath;
        String outputPath1, outputPath2;

        salesPath = args[0];
        motorbikesPath = args[1];
        outputPath1 = args[2];
        outputPath2 = args[3];

        SparkConf conf = new SparkConf().setAppName("Exam20210630 - Spark");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> salesRDD = sc.textFile(salesPath);
        JavaRDD<String> motorbikesRDD = sc.textFile(motorbikesPath);

        // Part 1
        // filter only year 2020 in EU sales
        JavaRDD<String> euSales2020RDD = salesRDD.filter(line -> {
            String[] fields = line.split(",");
            String date = fields[3];
            String eu = fields[6];
            String year = date.split("/")[0];

            return year.equals("2020") && eu.equals("T");
        });

        // map the String RDD into a PairRDD
        // (modelID, (price, price))
        JavaPairRDD<String, Tuple2<Double, Double>> modelPriceRDD = euSales2020RDD.mapToPair(line -> {
            String[] fields = line.split(",");
            String modelID = fields[2];
            Double price = Double.parseDouble(fields[5]);

            return new Tuple2<>(modelID, new Tuple2<>(price, price));
        });

        // use reduceByKey transformation to compute the min and max price for each modelID
        // by comparing for each modelID the minimum and maximum sale prices.
        // (modelID, (minPrice, maxPrice))
        JavaPairRDD<String, Tuple2<Double, Double>> modelMinMaxRDD = modelPriceRDD
        		.reduceByKey((i1, i2) -> {
        			double min = Math.min(i1._1(), i2._1());
        			double max = Math.max(i1._2(), i2._2());
        			return new Tuple2<>(min, max);
        		});

        // compute the variation in price and select only modelIDs with variation >= 5000
        // (modelID, variation)
        JavaPairRDD<String, Double> largeVarModelsRDD = modelMinMaxRDD
        						.mapValues(it -> it._2() - it._1())
        						.filter(it -> it._2() >= 5000);

        // select only the modelIDs and save as text file
        JavaRDD<String> result1RDD = largeVarModelsRDD.keys();
        result1RDD.saveAsTextFile(outputPath1);

        // Part 2
        // count for each motorbike models the number of sales
        JavaPairRDD<String, Integer> countSalesRDD = salesRDD.mapToPair(line -> {
            String[] fields = line.split(",");
            String modelID = fields[2];
            return new Tuple2<>(modelID, 1);
        }).reduceByKey((i1, i2) -> i1 + i2);
        
        // Select the motorbike models with more than 10 sales. 
        // They are neither unsold nor infrequently sold models.  
        JavaPairRDD<String, Integer> countSalesMoreThan10RDD =  countSalesRDD.filter(pair -> pair._2()>10);  

        // Map the motorbikes txt file into a pairRDD
        // containing (modelID, manufacturer)
        JavaPairRDD<String, String> modelManufacturerRDD = motorbikesRDD.mapToPair(line -> {
            String[] fields = line.split(",");
            String modelID = fields[0];
            String manufacturer = fields[2];

            return new Tuple2<>(modelID, manufacturer);
        });

        
        // Select from modelManufacturerRDD the motorbike models that are unsold or   
        // infrequently sold models
        JavaPairRDD<String, String> unsoldInfreqModelsManufRDD = modelManufacturerRDD
        		.subtractByKey(countSalesMoreThan10RDD);
        
        
        
        // Map the previously computed RDD (modelID, manufacturer)
        // into (Manufacturer, +1)
        // and count the number of infrequent + unsold motorbike models for each manufacturer
        // The result is (manufacturer, countUnsoldInfrequentMotorbikes)
        JavaPairRDD<String, Integer> manufacturerUnsoldInfreqCountRDD = unsoldInfreqModelsManufRDD
                                                                            .mapToPair(it -> new Tuple2<>(it._2(), 1))
                                                                            .reduceByKey((i1, i2) -> i1 + i2);

        // Filter only manufacturers with unsold + infrequent motorbike models >= 15
        JavaPairRDD<String, Integer> filteredManufacturersRDD = manufacturerUnsoldInfreqCountRDD
                                                                        .filter(it -> it._2() >= 3); //15, set 3 for testing

        filteredManufacturersRDD.saveAsTextFile(outputPath2);

        sc.close();
    }
}
