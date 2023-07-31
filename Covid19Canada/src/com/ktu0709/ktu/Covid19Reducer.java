package com.ktu0709.ktu;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class Covid19Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
	static Logger logger = Logger.getLogger(Covid19Reducer.class);
	 
	public void reduce(Text _key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int maxvalue = Integer.MIN_VALUE;
		
		
		// process values
		for (IntWritable val : values) {
           maxvalue = Math.max(maxvalue,val.get());			
		}
		context.write(_key, new IntWritable(maxvalue));		
	}

}
