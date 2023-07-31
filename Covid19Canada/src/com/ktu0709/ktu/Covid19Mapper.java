package com.ktu0709.ktu;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Covid19Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	static Logger logger = Logger.getLogger(Covid19Mapper.class);
	
	int iCaseTotal = 0;
	
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
    PropertyConfigurator.configure("log4j.properties");	
    Text date;
	String str_caseTotal;
	
	String line = ivalue.toString();
	String CovidData[] = line.split(",");
	
	date = new Text(CovidData[3]);
	
	//System.out.println("CovidData[3] : " + date);
	
	str_caseTotal = CovidData[8];
	
	//System.out.println("CovidData[8] : " + str_caseTotal);
	
	if(isNumberic(str_caseTotal) == true) {
	   iCaseTotal = Integer.parseInt(str_caseTotal);	
       context.write(date, new IntWritable(iCaseTotal));	
	 }
	}

	public static boolean isNumberic(String strNum) {
	  if(strNum == null) {
		return false;
	  }
	  try {
		  int d = Integer.parseInt(strNum);		  
	  } catch(NumberFormatException nfe) {
		  return false;
	  }
	  return true;
	}
	
}
