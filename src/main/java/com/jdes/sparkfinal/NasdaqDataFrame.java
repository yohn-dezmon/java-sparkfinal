package com.jdes.sparkfinal;


import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.io.File;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import java.util.List;
import java.util.ArrayList;
import java.lang.Math;
import java.math.BigDecimal;
import java.math.RoundingMode;


public class NasdaqDataFrame {
	
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
		
		// I need to understand what the config method does...
		SparkSession spark = SparkSession.builder().master("local").appName("Nasdaq Data").
				config("some config", "value").getOrCreate();
		
		// creates an empty list to add the dataframes to
		List<Dataset<Row>> arrayOfDfs = new ArrayList<Dataset<Row>>();
		
		Pattern nasdq = Pattern.compile(".*nasdaq\\.csv");
		
		// test dir = /home/vmuser/testSparkFinal/
		// actual dir = /home/vmuser/stockdata/
		File[] folder = new File(inputDir).listFiles();
		for (File file : folder) {
			String filePath = file.getAbsolutePath();
			String inputFile = file.getName();
			Matcher m = nasdq.matcher(inputFile);
			if (m.matches()) {
				createDataFrame(spark, filePath, inputFile, arrayOfDfs);
			}
		}
		
		// this udf rounds the volume float to the one's place
		spark.udf().register("deletezeroes", (String volume) -> {
			
			Double volInt = Double.parseDouble(volume);
			
			long rounded = Math.round(volInt);
			
			String roundedStr = Long.toString(rounded);
			
			return roundedStr;
			
		}, DataTypes.StringType);
		
		// this rounds the prices to two decimal places
		spark.udf().register("roundudf", (String number) -> {
			
			BigDecimal amount = new BigDecimal(number);
			BigDecimal roundedBigDeci = amount.setScale(2, RoundingMode.HALF_UP);
			String roundedStr = roundedBigDeci.toString();

			return roundedStr;
			
		}, DataTypes.StringType);

		
		// this parses the date to be in the correct format (yyyy-M-d) 
		// also the beginning takes the military time from the day I extracted the 
		// dataset and sets it equal to 2100/12/30 for later filtering
		spark.udf().register("todate", (String dateStr) -> {
		            if (dateStr.equals("") || dateStr.matches("(\\d{2}:\\d{2})")) {
		            	String madeUpDate = "2100/12/30";
		            	LocalDate badDate = LocalDate.parse(madeUpDate, 
		            						DateTimeFormatter.ofPattern("yyyy/M/d"));
		            	String badDateStr = badDate.toString();
		            			
		                return badDateStr;
		            } 
		                // Example input: 2019/07/08
		                LocalDate goodDate = LocalDate.parse(dateStr,
		                DateTimeFormatter.ofPattern("yyyy/M/d"));
		                
		                String goodDateStr = goodDate.toString();

		                
		            
		            return goodDateStr;
		            
		        }, DataTypes.StringType);
		
		

		
		for (Dataset<Row> dataframe : arrayOfDfs) {
			// this creates a table nasdaq that I can run sql queries on
			dataframe.createOrReplaceTempView("nasdaq");
			
			// this dataframe stores the normalized table in which date and volume were altered using
			// udfs
			Dataset<Row> dateFormatted = dataframe.sqlContext().sql("Select todate(date) as date, company, "
					+ "roundudf(open) as open, roundudf(close) as close, roundudf(high) as high, roundudf(low) as low," 
					+ " deletezeroes(volume) as volume from nasdaq");
			
			// I'm not sure if this step is necessary 
			dateFormatted.createOrReplaceTempView("nasdaq");
			// filters out the bad date that I created in the todate udf
			Dataset<Row> filtered = dateFormatted.filter(dateFormatted.col("date").startsWith("2018").or(dateFormatted.col("date").startsWith("2019")));
			
//			Dataset<Row> ordered = filtered.orderBy(filtered.col("date").asc());
			// this prints out the first 20 rows of each table being processed for tracking purposes
//			ordered.show();
			
			// local output: /home/vmuser/stockdata/joineddata/nasdaq
//			 this saves individual parquet files into a folder, that can later be accessed as one dataframe
			filtered.write().mode(SaveMode.Append).parquet(output);

		}
		
		
		
	}
	
	public static void createDataFrame(SparkSession spark, String filePath, String inputFile,
										List<Dataset<Row>> arrayOfDfs) {
		// this method extracts the abbreviation of the company and adds it to a dataframe
		// column once the csv has been read in from the spark session.
		Pattern abbr = Pattern.compile("([A-Z]+)");
		Matcher m = abbr.matcher(inputFile);
		String abbreviation;
			if (m.find()) {
				abbreviation = m.group(0);
				
				Dataset<Row> df = spark.read().format("csv").option("header", "true").load(filePath);
		
				Dataset<Row> dfWithName = df.withColumn("company", functions.lit(abbreviation));
				
				// I add each array to an arraylist to be normalized later in the code
				arrayOfDfs.add(dfWithName);
				
			}
			
		}
		
		
		
	
		
		
	}
	


