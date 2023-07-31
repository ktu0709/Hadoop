package com.ktu0709.ktu;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    static Logger logger = Logger.getLogger(WordMapper.class);
	private final IntWritable one = new IntWritable(1);
	
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
       
	   logger.info("ikey : " + ikey + ", ivalue :" + ivalue);
		
	   String line = ivalue.toString();
       String words[]=line.split(" ");
       
      for(String word:words) {
    	  if(!(word.toString().isEmpty())) //except Blank
      context.write(new Text(word), one);  
      logger.info("word :" + word + "one :" + one);
	  }
      
  }

}
