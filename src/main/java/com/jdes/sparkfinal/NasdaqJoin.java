package com.jdes.sparkfinal;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


public class NasdaqJoin {
	
	public static void main(String[] args) {
		String yahooDir;
		String nasdaqDir;
		String output;
		String outputcsv;
		if (args.length == 4) {
			yahooDir = args[0];
			nasdaqDir = args[1];
			output = args[2];
			outputcsv = args[3];
		} else {
			System.out.println("Error with commandline inputs!");
			return;
		}
		
		SparkSession spark = SparkSession.builder().master("local").appName("Nasdaq DataJoin").
				config("some-config", "some-value").getOrCreate();
		
		Dataset<Row> yahoodf = spark.read().load(yahooDir);
		yahoodf.createOrReplaceTempView("yahoo");
		
		Dataset<Row> nasdaqdf = spark.read().load(nasdaqDir);
		nasdaqdf.createOrReplaceTempView("nasdaq");
		
		// Here I join the two datasets created in NasdaqDataFrame and YahooDataFrame
		Dataset<Row> joinedOpenClose = spark.sqlContext().sql("select date, company, open, close from ("
				+ "(Select date, company, open, close from yahoo) union all "
				+ "(select date, company, open, close from nasdaq)) as joined order by joined.company, joined.date");
		
		joinedOpenClose.show(1000);
		// create a new temporary table to query
		joinedOpenClose.createOrReplaceTempView("joined");
		
		
		// this calculates a running average for the opening and close prices daily for a total of 251
		// days from 07/09/2018 to 07/08/2019. 251 because the stock market isn't always open.
		Dataset<Row> runningOpenCloseAvg251 = spark.sqlContext().sql("Select company, date, sum(open) as OpenPrice, sum(close) as ClosePrice, "
				+ " avg(sum(open)) over (partition by company order by date rows between 250 preceding and current row) as MAO251, "
				+ "avg(sum(close)) over (partition by company order by date rows between 250 preceding and current row) as MAC251 from"
				+ " joined group by company, date");
		
		runningOpenCloseAvg251.show(1500);
		
		runningOpenCloseAvg251.write().mode(SaveMode.Overwrite).parquet(output);
		// I saved to csv just for the purposes of verifying that the output was correct
		// in production, parquet would be used as it is much more efficient.
		runningOpenCloseAvg251.write().mode(SaveMode.Overwrite).csv(outputcsv);
		
	}
	

}
