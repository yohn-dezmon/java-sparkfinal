package com.jdes.sparkfinal;

import static org.junit.Assert.*;
import org.junit.Test;
import com.jdes.sparkfinal.MethodsForUnitTests;
import static org.junit.Assert.*;
import java.lang.AssertionError;
import java.util.List;
import java.util.ArrayList;

public class MethodsTest {

	@Test
	public void RegexTest() {
		String[] array = new String[4];
		
		array[1] = "nasdaq.csv";
		array[0] = 	"XRXnasdaq.csv";
		array[2] = "XRX.csv";
		array[3] = "home/vmuser/stockdata/TGTnasdaq.csv";
		
		int result = MethodsForUnitTests.checkingRegexImpl(array);
		
		assertEquals(3, result);
	}
	
	@Test
	public void RegexTestBad() {
		String[] array = new String[4];
		
		array[1] = "nasdaq.csv/stockdata";
		array[0] = 	"XRXnasdaq.csv";
		array[2] = "XRX.csv";
		array[3] = "home/vmuser/stockdata/TGTnasdq.csv";
		
		int result = MethodsForUnitTests.checkingRegexImpl(array);
		
		assertEquals(1, result);
	}
	
	@Test
	public void regexExtractTest() {
		String[] array = new String[2];
		
		
		array[0] = 	"XRXnasdaq.csv";
		array[1] = "nasdaq.csv";

		
		String result = MethodsForUnitTests.regexExtractAbbr(array);
		
		assertEquals("XRX", result);
		
	}
	
	@Test
	public void bigDeciTest() {
		String numb = "188.047531";
		
		String result = MethodsForUnitTests.bigDecimalRound(numb);
		
		assertEquals("188.05", result);
	}
	
	@Test(expected=AssertionError.class)
	public void bigDeciTestBad() {
		String numb = "188.047531";
		
		String result = MethodsForUnitTests.bigDecimalRound(numb);
		
		assertEquals("188.04", result);
	}

}
