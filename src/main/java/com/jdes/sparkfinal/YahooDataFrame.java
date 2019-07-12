package com.jdes.sparkfinal;

import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.functions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.File;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;

import java.util.List;
import java.util.ArrayList;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class YahooDataFrame {
	

	public static void main(String[] args) {
		String inputDir;
		String output;
		if (args.length == 2) {
			inputDir = args[0];
			output = args[1];
		} else {
			System.out.println("Error with commandline inputs!");
			return;
		}
		
		SparkSession spark = SparkSession.builder().master("local").appName("Nasdaq Data").
				config("some config", "value").getOrCreate();
		
		// creates an empty list to add the dataframes to
		List<Dataset<Row>> arrayOfDfs = new ArrayList<Dataset<Row>>();
		

		Pattern yahoo = Pattern.compile("([A-Z]+)(\\.csv)");
		
		// test dir = /home/vmuser/testSparkFinal/
		// actual dir = /home/vmuser/stockdata/
		File[] folder = new File(inputDir).listFiles();
		for (File file : folder) {
			String filePath = file.getAbsolutePath();
			String inputFile = file.getName();
			Matcher m = yahoo.matcher(inputFile);
			if (m.matches()) {
				String abbreviation = m.group(1);
				createDataFrame(spark, filePath, inputFile, arrayOfDfs, abbreviation);
			}
		}
		
		// this udf rounds the volume float to the one's place
		spark.udf().register("roundudf", (String number) -> {
		
			BigDecimal amount = new BigDecimal(number);
			BigDecimal roundedBigDeci = amount.setScale(2, RoundingMode.HALF_UP);
			String roundedStr = roundedBigDeci.toString();
		
			return roundedStr;
					
			}, DataTypes.StringType);
	
		for (Dataset<Row> dataframe : arrayOfDfs) {
			// this creates a table nasdaq that I can run sql queries on
			dataframe.createOrReplaceTempView("yahoo");
					
			// this dataframe stores the normalized table in which date and volume were altered using
			// udfs
			Dataset<Row> yahooTable = dataframe.sqlContext().sql("Select date, company, "
							+ "roundudf(open) as open, roundudf(close) as close, roundudf(high) as high, roundudf(low) as low," 
							+ " volume from yahoo LIMIT 251");
					
					
			// this prints out the first 20 rows of each table being processed for tracking purposes
			yahooTable.show();

			// this saves individual parquet files into a folder, that can later be accessed as one dataframe
			yahooTable.write().mode(SaveMode.Append).parquet(output);

				}
				
				
				
	}
	
	public static void createDataFrame(SparkSession spark, String filePath, String inputFile,
			List<Dataset<Row>> arrayOfDfs, String abbreviation) {

			Dataset<Row> df = spark.read().format("csv").option("header", "true").load(filePath);
			
			// this adds a column for the company's abbreviation, i.e. XRX (Xerox)
			Dataset<Row> dfWithName = df.withColumn("company", functions.lit(abbreviation));

			// I add each array to an arraylist to be normalized later in the code
			arrayOfDfs.add(dfWithName);

		}
	




}
	

