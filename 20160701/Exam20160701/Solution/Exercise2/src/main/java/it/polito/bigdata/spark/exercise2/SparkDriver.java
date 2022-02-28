package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import org.apache.spark.SparkConf;
	
public class SparkDriver {
	
	
	public static void main(String[] args) {

		String inputPathBooks;
		String inputPathBoughtBooks;
		String outputPathNeverSold;
		String outputPathCheckPurchases;

		double threshold;
		
		inputPathBooks=args[0];
		inputPathBoughtBooks=args[1];
		threshold=Double.parseDouble(args[2]);
		outputPathNeverSold=args[3];
		outputPathCheckPurchases=args[4];
		
	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Exam 2016_07_01 - Exercise #2");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// *****************************************
		// Exercise 2 - Part A
		// *****************************************

		// Read the content of books.txt
		JavaRDD<String> booksRDD = sc.textFile(inputPathBooks);

		// Select expensive books
		JavaRDD<String> expensiveBooksRDD = booksRDD.filter(new Expensive());
		
		// Select the ids of the expensive books
		JavaRDD<String> IDsExpensiveBooksRDD = expensiveBooksRDD.map(new BookId());		
		
		// Read the content of boughtBooks.txt
		JavaRDD<String> boughtBooksRDD = sc.textFile(inputPathBoughtBooks).cache();
		
		// Select the ids of the bought books
		JavaRDD<String> IDsBoughtBooksRDD = boughtBooksRDD.map(new BookIdFromPurchase());		
		
		// Compute the difference (apply subtract)  between the ids of the expensive books and 
		// those of the bought books. Only the ids of the expensive never-sold books are returned
		JavaRDD<String> ExpensiveNeverSoldRDD = IDsExpensiveBooksRDD.subtract(IDsBoughtBooksRDD);
		
		
		// Store the selected users in the output folder
		ExpensiveNeverSoldRDD.saveAsTextFile(outputPathNeverSold);
		
		System.out.println("Number of expensive never-sold books: "+ ExpensiveNeverSoldRDD.count());
		
		// *****************************************
		// Exercise 2 - Part B
		// *****************************************
		// Count for each customer the number of purchases and 
		// the number of cheap purchases
		// This is a word count-like problem. To improve the efficiency
		// of the application we can associate to each customer a 
		// complex counter containing two integer values: num. purchases and num. cheap purchases   
		
		// For each line of BoughtBooks, the program emits one pair
		// key = customerid
		// value = ComplexCount 
		// 			- numPurchases=1, numCheapPurchases=1 if the purchase is cheap
		// 			- numPurchases=1, numCheapPurchases=0 if the purchase is not cheap
		
		JavaPairRDD<String,ComplexCount> CustomerCountRDD=boughtBooksRDD.mapToPair(new PairCustomerCount());
		
		// Count the total number of purchases and cheap purchases for each customer
		JavaPairRDD<String,ComplexCount> TotalCustomerCountRDD=CustomerCountRDD.reduceByKey(new Sum());
		
		// Select the users with a propensity to cheap purchases  
		JavaPairRDD<String,ComplexCount> SelectedCustomers=TotalCustomerCountRDD.filter(new Propensity(threshold));
		
		// Select the keys and store them
		SelectedCustomers.keys().saveAsTextFile(outputPathCheckPurchases);

		// Close the Spark context
		sc.close(); 
	}
}
