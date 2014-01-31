import java.io.BufferedReader;

import java.util.*;

import java.lang.*;

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
             String line = value.toString();
        	 String splitone[]=line.split(",");
        	   
        	    
				
        	    String x5 =splitone[3] ;
                String y5=splitone[4];
                String interest=splitone[2];

                
                
                
                int x4=Integer.parseInt(x5);
                int y4=Integer.parseInt(y5);
        	
        		
                
                
                if(check(x4,y4,interest) == 1)
				
        	    {
				Text skey=new Text(interest);
				context.write(skey,one);
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
	
	public static int check(int x,int y,String interest)
	{
	 int x1=0;
	 int y1=0;
	 
	 
	 if (interest.equals("Clothing"))
		{
		     x1=33;
			 y1=98;
			
		}
		if (interest.equals("Cooking"))
		{    
		     x1=33;
			 y1=78;
			
		}
		if (interest.equals("Cosmetics"))
		{
		     x1=53;
			 y1=85;
			
		}
		if (interest.equals("Cruise vacation"))
		{
		     x1=79;
			 y1=50;
			
		}
		if (interest.equals("Electronics"))
		{
		     x1=13;
			 y1=69;
			
		}
		if (interest.equals("Fashion"))
		{
		     x1=48;
			 y1=79;
			
		}
		if (interest.equals("Fishing"))
		{
		     x1=88;
			 y1=33;
			
		}
		if (interest.equals("Fitness"))
		{
		     x1=4;
			 y1=22;
			
		}
		
		if (interest.equals("Bicycling"))
		{   
		     x1=82;
			 y1=59;
			
		}
		if (interest.equals("Boating sailing"))
		{   
		     x1=86;
			 y1=22;
			
		}
		if (interest.equals("Book reading"))
		{
		     x1=31;
			 y1=23;
			
		}
		if (interest.equals("Aircraft"))
		{
		     x1=56;
			 y1=74;
			
		}
		if (interest.equals("Art antique collecting"))
		{    x1=93;
			 y1=39;
			
		}
		if (interest.equals("Astrology"))
		{
		     x1=60;
			 y1=1;
			
		}
		if (interest.equals("Baseball"))
		{
		     x1=36;
			 y1=85;
			
		}
		if (interest.equals("Basketball"))
		{
		     x1=60;
			 y1=49;
			
		}
		
		if (interest.equals("Camping hiking"))
		{
		     x1=22;
			 y1=73;
			
		}
		if (interest.equals("Casino vacation"))
		{
		     x1=34;
			 y1=25;
			
		}
		
		if (interest.equals("Football"))
		{
		     x1=90;
			 y1=32;
			
		}
		if (interest.equals("Golf"))
		{
		     x1=33;
			 y1=55;
			
		}
		
		if (interest.equals("Shoes"))
		{
		     x1=40;
			 y1=58;
			
		}
		if (interest.equals("Swimming pool"))
		{
		     x1=41;
			 y1=14;
			
		}
		if (interest.equals("Tennis"))
		{
		    x1=85;
			 y1=74;
			
		}
	
	
		if (interest.equals("History"))
		{
		     x1=32;
			 y1=70;
			
		}
		if (interest.equals("Hockey"))
		{    x1=67;
			 y1=75;
			
		}
		if (interest.equals("Home furnishings"))
		{
		     x1=19;
			 y1=14;
			
		}
		if (interest.equals("Household pets"))
		{
		     x1=9;
			 y1=91;
			
		}
		if (interest.equals("Hunting"))
		{
		     x1=81;
			 y1=63;
			
		}
		if (interest.equals("Jewelry"))
		{
		     x1=52;
			 y1=35;
			
		}
		if (interest.equals("Scuba"))
		{
		     x1=9;
			 y1=56;
			
		}
		
    double temp1=Math.pow((x1-x), 2);	
    double temp2=Math.pow((y1-y),2);
    double dist=Math.sqrt(temp1+temp2);
    
    if(dist <= 5)
	return 1;
	else
	return 0;
	
	}
	
}	
