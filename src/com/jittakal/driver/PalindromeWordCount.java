package com.jittakal.driver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.jittakal.mapper.PalindromeWordCountMapper;
import com.jittakal.reducer.PalindromeWordCountReducer;

public class PalindromeWordCount {

	public static void main(String[] args) throws Exception{
		JobClient client = new JobClient();
		JobConf conf = new JobConf(PalindromeWordCount.class);
		conf.setJobName("palindromewordcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(PalindromeWordCountMapper.class);
		conf.setCombinerClass(PalindromeWordCountReducer.class);
		conf.setReducerClass(PalindromeWordCountReducer.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		client.setConf(conf);
		JobClient.runJob(conf);
	}

}
