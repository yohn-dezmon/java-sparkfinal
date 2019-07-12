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


## Sample Output with Opening and Closing Price Moving Averages
company-date----open--close--OpenMovingAvg---CloseMovingAvg--  
  
LYFT,2019-06-26,64.32,62.97,60.63161290322581,60.23951612903228  
LYFT,2019-06-27,63.52,65.27,60.677460317460316,60.3193650793651  
LYFT,2019-06-28,65.2,65.71,60.748124999999995,60.40359375000002  
LYFT,2019-07-01,66.0,62.0,60.82892307692307,60.42815384615387  
LYFT,2019-07-02,62.89,60.35,60.86015151515151,60.426969696969714  
LYFT,2019-07-03,60.64,60.6,60.856865671641785,60.42955223880599  