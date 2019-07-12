package com.jdes.sparkfinal;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.math.BigDecimal;
//import java.text.NumberFormat;
//import java.util.Locale;
import java.math.RoundingMode;

public class MethodsForUnitTests {
	
	
public static int checkingRegexImpl(String[] array) {
		
		Pattern nasdq = Pattern.compile(".*nasdaq\\.csv");
		int count = 0;
		
		for (String filename : array) {
			Matcher m = nasdq.matcher(filename);
			if (m.matches()) {
				count += 1;
			} 
		}
		
		return count;
	}

	public static String regexExtractAbbr(String[] array) {
		
		Pattern abbr = Pattern.compile("([A-Z])+");
		
		
		for (String filename : array) {
			Matcher m = abbr.matcher(filename);
			if (m.find()) {
				return m.group(0);
			}
			
		}
		return "none";
		
	}
	
	public static String bigDecimalRound(String number) {
		
		BigDecimal bigDecNum = new BigDecimal(number);
//		NumberFormat nf = NumberFormat.getCurrencyInstance(Locale.US);
//		String formatNum = nf.format(bigDecNum);
		
		BigDecimal bd = bigDecNum.setScale(2, RoundingMode.HALF_UP);
		
		String formatNum = bd.toString();
		
		return formatNum;
		
	}

}
