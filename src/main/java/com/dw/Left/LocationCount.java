package com.dw.Left;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class LocationCount {

	public static class LocationCountMapper extends Mapper<LongWritable, Text, Text, Text> {  
	    private Text outputKey = new Text();  
	    private Text outputValue = new Text();  
      
	    @Override  
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  
			String [] tokens = value.toString().split("\t");      
			outputKey.set(tokens[0]);  
			outputValue.set( tokens[1]);  
			context.write(outputKey,outputValue);  
	    }  
	} 
	
	public static class LocationCountReducer extends Reducer<Text, Text, Text, Text> {  
  
	    @Override  
	    public void reduce(Text productID, Iterable<Text> locations, Context context) throws  IOException, InterruptedException {   
	        Set<String> set = new HashSet<String>();    
	        for (Text location: locations) {  
	           set.add(location.toString());  
	        }   
	        context.write(productID, new Text(set.toString()));  
	    }  
	}  
	
	public static void main( String[] args ) throws Exception {  
        
	      Path input = new Path("hdfs://localhost:9000/user/dw/output/1/");  
	      Path output = new Path("hdfs://localhost:9000/user/dw/output/2");  
	      Configuration conf = new Configuration();  
	  
	      Job job = new Job(conf);  
	      job.setJarByClass(LocationCount.class);  
	      job.setJobName("LocationCount");  
	      
	      FileInputFormat.addInputPath(job, input);  
	      job.setInputFormatClass(TextInputFormat.class);  
	          
	      job.setMapperClass(LocationCountMapper.class);  
	      job.setReducerClass(LocationCountReducer.class);  
	  
	      job.setMapOutputKeyClass(Text.class);  
	      job.setMapOutputValueClass(Text.class);  
	  
	      job.setOutputFormatClass(TextOutputFormat.class);  
	      job.setOutputKeyClass(Text.class);  
	      job.setOutputValueClass(Text.class);  
	      
	      FileOutputFormat.setOutputPath(job, output);  
	      if (job.waitForCompletion(true)) {  
	        return;  
	      }  
	      else {  
	          throw new Exception("LocationCountDriver Failed");  
	      }  
	 }  
}
