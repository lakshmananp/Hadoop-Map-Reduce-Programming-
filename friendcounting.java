import java.io.BufferedReader;

import java.io.File;

import java.io.FileInputStream;

import java.io.IOException;

import java.io.InputStream;

import java.io.InputStreamReader;

import java.util.Iterator;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class lak
{

public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> 
	{
		private final static IntWritable one = new IntWritable(1);
		
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {    
		    
		    String x=null;
            String y=null;
			
			String line = value.toString();
     
        	 String t[] = line.split(",");
        	
			x=t[0];
        	y=t[1];
        	
			
			
			if (x != null && y!= null)
        	{
        		Text f1 = new Text(x);
        		context.write(f1, one);
        	

     			Text f2=new Text(y);
				context.write(f2, one);
        	}
        }
    } 
 
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> 
    {
    	
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
        {
            int ct = 0;
            
			for (IntWritable value : values) 
            {
            	int val = value.get();
            	ct += val;
			}
            
            context.write(key, new IntWritable(ct));
        }
    }
	
	

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException 
	{
		Job job = new Job();
		job.setJarByClass(lak.class);
		job.setJobName("lak");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.waitForCompletion(true);
		
	}
}
