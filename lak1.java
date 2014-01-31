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
           String line = value.toString();
        	StringTokenizer t = new StringTokenizer(line, ",");
        	    int count = 0;
        	    
				String age = null;
        	    String x = null;
                String y=null;
                String interest=null;
                String userid=null;
                
				String range1="a(5-14)";
                String range2="b(15-24)";
                String range3="c(25-34)";            	
                String range4="d(35-44)";
                String range5="e(45-54)";
                String range6="f(55+)";  
        	while (t.hasMoreTokens())
        	{
        		String temp = t.nextToken();
        		
        		if (count == 0)
        		{
        			userid = temp;
        		}
        		if (count == 1)
        		{
        			age= temp;
        		}
                 if(count ==2 )
                 {
                      interest=temp;
                 }

                 if(count ==3)
                  {
                     x=temp;
                  }
                  if(count ==4)
                   {
                       y=temp;
                   }
        		count++;
        	}


        	int tempo=Integer.parseInt(age);
       
               if(tempo >=5 && tempo <=14 )
               {
                 Text s= new Text(range1);
        		context.write(s, one);
                }
               if(tempo >=15 && tempo <=24 )
                {Text s= new Text(range2);
        		context.write(s, one);

                 }
               if(tempo >=25 && tempo <=34 )
                 {Text s = new Text(range3);
        		context.write(s, one);

                  }
               if(tempo >=35 && tempo <=44 )
                {Text s = new Text(range4);
        		context.write(s, one);

                 }
               if(tempo >=45 && tempo <=54 )
                 {Text s = new Text(range5);
        		context.write(s, one);
                  }
               if(tempo >= 55)
                  {
                Text s = new Text(range6);
        		context.write(s, one);

                   }

        		//Text friend = new Text(friend1);
        		//context.write(friend, one);
        		
        		
        }
    } 
 
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> 
    {
    	
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
        {
            int count = 0;
            for (IntWritable value : values) 
            {
            	int val = value.get();
            	count += val;
			}
            
            context.write(key, new IntWritable(count));
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
