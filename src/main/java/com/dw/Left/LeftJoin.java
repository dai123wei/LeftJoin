package com.dw.Left;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class LeftJoin {
	
	public static class PairOfStrings implements WritableComparable<PairOfStrings>{

		private String flag;
		private String value;
		
		public String getFlag() {
			return flag;
		}
		
		public String getValue() {
			return value;
		}
		
		public void set(String flag, String value) {
			this.flag = flag;
			this.value = value;
		}
		
		public void write(DataOutput out) throws IOException {
			out.writeUTF(flag);
			out.writeUTF(value);
		}
		
		public void readFields(DataInput in) throws IOException {
			flag = in.readUTF();
			value = in.readUTF();
		}
		
		public int compareTo(PairOfStrings o) {
			int mini = flag.compareTo(o.flag);
			if(mini == 0) {
				mini = value.compareTo(o.value);
			}
			return mini;
		}
		
		public boolean equals(Object object) {
			if(object == null)
				return false;
			if(this == object) 
				return true;
			if(object instanceof PairOfStrings) {
				PairOfStrings o = (PairOfStrings) object;
				return flag.compareTo(o.flag) == 0 && value.compareTo(o.value) == 0;
			}else {
				return false;
			}
		}
		
		public String toString() {
			return flag + "\t" + value;
		}
		
	}
	
	public static class LeftJoinUserMapper extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {  
		PairOfStrings outputKey = new PairOfStrings();  
		PairOfStrings outputValue = new PairOfStrings();  
		public void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException { 
			//System.out.println("LeftJoinUserMapper---------------------------------");
			String[] tokens = StringUtils.split(value.toString(), "\t");  
			if (tokens.length == 2) {  
				// to make sure location arrives before products  
				outputKey.set(tokens[0], "1");    // set user_id  
				outputValue.set("L", tokens[1]);  // set location_id  
				context.write(outputKey, outputValue);  
			}  
		}  
	  
	}
	
	public static class LeftJoinTransactionMapper extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {  
      
		PairOfStrings outputKey = new PairOfStrings();  
		PairOfStrings outputValue = new PairOfStrings();  
		
		public void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {  
			//System.out.println("LeftJoinTransactionMapper---------------------------------");
			String[] tokens = StringUtils.split(value.toString(), "\t");  
			//System.out.println("tokens size:" + tokens.length);  
			  
			String productID = tokens[1];  
			String userID = tokens[2];  
			// make sure products arrive at a reducer after location  
			outputKey.set(userID, "2");  
			outputValue.set("P", productID);  
			context.write(outputKey, outputValue);  
		}  
  
	} 
	
	public static class SecondarySortGroupComparator extends WritableComparator{
		protected SecondarySortGroupComparator() {
			super(PairOfStrings.class, true);
			System.out.println("SecondarySortGroupComparator---------------------------------");
		}
		
		public int compare(WritableComparable a, WritableComparable b) {
			PairOfStrings apair = (PairOfStrings) a;
			PairOfStrings bpair = (PairOfStrings) b;
			
			return apair.getFlag().compareTo(bpair.getFlag());
		}
	}
	
	public static class LeftJoinReducer extends Reducer<PairOfStrings, PairOfStrings, Text, Text> {  
  
	   Text productID = new Text();  
	   Text locationID = new Text("undefined");  
	     
	   @Override  
	   public void reduce(PairOfStrings key, Iterable<PairOfStrings> values, Context context) throws IOException, InterruptedException {  
			System.out.println("LeftJoinReducer---------------------------------");
			//System.out.println("key=" + key);  
			Iterator<PairOfStrings> iterator = values.iterator();  
			System.out.println("values");  
			if (iterator.hasNext()) {  
				// firstPair must be location pair  
				PairOfStrings firstPair = iterator.next();   
				//System.out.println("firstPair="+firstPair.toString());  
				if (firstPair.getFlag().equals("L")) {  
					locationID.set(firstPair.getValue());  
				}  
			}        
               
			while (iterator.hasNext()) {  
				// the remaining elements must be product pair  
				PairOfStrings productPair = iterator.next();   
				//System.out.println("productPair="+productPair.toString());  
				productID.set(productPair.getValue());  
				context.write(productID, locationID);  
			}  
		}  
	  
	}
	
	public static void main( String[] args ) throws Exception {  
	      Path transactions = new Path("hdfs://localhost:9000/user/dw/input/transactions.txt");// input  
	      Path users = new Path("hdfs://localhost:9000/user/dw/input/users.txt");        // input 
	      String time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
	      Path output = new Path("hdfs://localhost:9000/user/dw/mr-leftjoin-" + time);        // output 
	  
	      Configuration conf = new Configuration(true);
 
	      Job job = new Job(conf, "LeftJoin");
	      job.setJarByClass(LeftJoin.class);
        
	      // 分组函数
	      job.setGroupingComparatorClass(SecondarySortGroupComparator.class);
 
	      // Reducer类型
	      job.setReducerClass(LeftJoinReducer.class);
 
	      // map输出Key的类型
	      job.setMapOutputKeyClass(PairOfStrings.class);
	      // map输出Value的类型
	      job.setMapOutputValueClass(PairOfStrings.class);
	      // reduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
	      job.setOutputKeyClass(Text.class);
	      // reduce输出Value的类型
	      job.setOutputValueClass(Text.class);
 
	      // 将输入的数据集分割成小数据块splites，同时提供一个RecordReder的实现。
	      job.setInputFormatClass(TextInputFormat.class);
	      // 提供一个RecordWriter的实现，负责数据输出。
	      job.setOutputFormatClass(TextOutputFormat.class);
        
	      //Mapper类型
	      MultipleInputs.addInputPath(job, transactions, TextInputFormat.class, LeftJoinTransactionMapper.class);
	      MultipleInputs.addInputPath(job, users, TextInputFormat.class, LeftJoinUserMapper.class);
	      FileOutputFormat.setOutputPath(job, output);
 
	      // 提交job
	      if (job.waitForCompletion(false)) {
	    	  System.out.println("job ok !");
	      } else {
	    	  System.out.println("job error !");
	      }
	  }  
}
