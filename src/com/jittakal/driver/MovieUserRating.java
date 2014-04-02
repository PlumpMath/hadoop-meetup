package com.jittakal.driver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.jittakal.mapper.MovieUserRatingMapper;
import com.jittakal.reducer.MovieUserRatingReducer;

public class MovieUserRating {

	public static void main(String[] args) throws Exception{
		JobClient client = new JobClient();
		JobConf conf = new JobConf(MovieUserRating.class);
		conf.setJobName("movieuserrating");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MovieUserRatingMapper.class);
		//conf.setCombinerClass(UserRatingReducer.class);
		conf.setReducerClass(MovieUserRatingReducer.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		client.setConf(conf);
		JobClient.runJob(conf);
	}

}
