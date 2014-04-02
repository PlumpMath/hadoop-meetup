package com.jittakal.reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MovieUserRatingReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		// Todo: incomplete
		
		String uname=null;
		String mid=null;
		String rating="0";
		while (values.hasNext()) {
			String value=values.next().toString();
			if(value.contains("$")){
				StringTokenizer tokenizer=new StringTokenizer(value,"$");
				if(tokenizer.hasMoreTokens())
					mid=tokenizer.nextToken();
				if(tokenizer.hasMoreTokens())
					rating=tokenizer.nextToken();
			}else{
				uname=value;
			}			
		}
		if(Double.parseDouble(rating)>=4.0){
			output.collect(key, new Text(uname+","+mid+","+rating));
		}
	}

}
