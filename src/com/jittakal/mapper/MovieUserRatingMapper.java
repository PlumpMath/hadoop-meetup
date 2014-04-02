package com.jittakal.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MovieUserRatingMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {	
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		// Todo: incomplete
		String line=value.toString();
		StringTokenizer tokenizer=new StringTokenizer(line,",");
		
		if(tokenizer.countTokens()==2){ // Users File
			String userid=tokenizer.nextToken();
			String uname=tokenizer.nextToken();
			output.collect(new Text(userid), new Text(uname));
		}else if(tokenizer.countTokens()==3){ // User Ratings File
			String uid=tokenizer.nextToken();
			String mid=tokenizer.nextToken();
			String rating=tokenizer.nextToken();
			output.collect(new Text(uid), new Text(mid+"$"+rating));			
		}		
	}

}
