package com.ktu0709.ktu;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	static Logger logger = Logger.getLogger(WordMapper.class);
	
	public void reduce(Text _key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {		
		
		logger.info("_key : " + _key + ", values :" + values);
		
		int total = 0;
		
		// process values
		for (IntWritable val : values) {
         total += val.get();
         logger.info("val : " + val + ", values :" + total);
		}
		
		context.write(_key, new IntWritable(total));
	}

}
