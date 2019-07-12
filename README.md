# Spark Stock Running Averages

This is a project I created from scratch that uses stock data from  
Yahoo Finance and Nasdaq websites to calculate running Open Price and  
Close Price averages for 21 different companies. 

The project begins in the **NasdaqDataFrame** and **YahooDataFrame** classes  
where the data is normalized, and columns are added to refer to the company  
for which the prices refer to.  

The project terminates in the **NasdaqJoin** class where the two datasets from  
Yahoo and Nasdaq are combined and used to calculate the running averages.

I used parquet files to store the datasets, with the exception of the final  
dataset stored as a CSV file as well to verify the correct output.

