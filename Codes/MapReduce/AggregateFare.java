/*
File Purpose: The following code implemets the Map Reduce logic
to calculate the minimum & maximum value of taxi fare by month and year.

Author: Devang Swami

Instructions: 
To compile the program: bin/hadoop com.sun.tools.javac.Main AggregateFare.java
To create JAR for executables: jar cf aggFare.jar AggregateFare*.class
Execute as Map Reduce Job: $HADOOP_HOME/bin/hadoop jar aggFare.jar AggregateFare input_path output_path

Notes:
Please note that this file has all the required functionalities 
required to implement the mentioned purpose.


*/


//Java libraries for common utilities and exception 
import java.io.IOException;
import java.util.StringTokenizer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.*;

//Java Libraries for Map Reduce framework
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AggregateFare {
	
	//Mapper program to tokenize the data into format <year-month, fare>
	public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		Text date = new Text();
		DoubleWritable Totalfare;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//Take one line at a time to process and split the data based on separator ","
            String line = value.toString();
            String[] ParsedLine = line.split(",");
            
			//Check for the line containing the column names (since data is in csv) and do not use it to generate key-value pairs
            if(!ParsedLine[0].equals("VendorID") && !ParsedLine[0].equals("")){
			    //Get date and fare from the tokenized data
				SimpleDateFormat date_format = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss");
                //The following code is in try-catch so as to handle Parse Exceptions
				try{
			        Date d = date_format.parse(ParsedLine[1]);
			        Calendar calendar = Calendar.getInstance(TimeZone.getDefault());
			        calendar.setTime(d);
                    date.set(((int) calendar.get(Calendar.MONTH) + 1)+"-" + calendar.get(Calendar.YEAR));
                    Totalfare = new DoubleWritable(Double.parseDouble(ParsedLine[16]));
                    //Write the results as key-value pair
                    context.write(date, Totalfare);
                } 
                catch(ParseException pe){
                    throw new IOException(pe);
                }
            }
		}
	}
	
	//Implemenation of reducer
	public static class MinMaxReducer extends Reducer<Text, DoubleWritable, Text, Text> {
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			//Global storage variables to store minimum and maximum values
			double min = Integer.MAX_VALUE, max = 0;
            //Iterator to iterate over values belonging to same key.
			Iterator<DoubleWritable> iterator = values.iterator(); 
	        //Calculate minimum & maximum values for each key
			while (iterator.hasNext()) {
		            double value = iterator.next().get();
		            if (value < min && value > 0) { 
			            min = value;
		            }
		            if (value > max && value > 0) { 
			            max = value;
		            } 
	        }
	        //Write results back as key value pairs
	        context.write(new Text(key), new Text(", " + min + ", " + max));
		}
	}

	public static void main(String[] args) throws Exception {
		
		//Configuration for Map Reduce jobs
		Configuration conf = new Configuration();
		
		//Comment or uncomment next two lines to enable intermediate compression
		conf.set("mapreduce.map.output.compress", "true");
		conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
		
		//Comment or uncomment to set Input Split size for Map Reduce
		// conf.setLong(FileInputFormat.SPLIT_MAXSIZE, conf.getLong( FileInputFormat.SPLIT_MAXSIZE, DEFAULT_SPLIT_SIZE) / 2);
		
		//Set map reduce job details
		Job job = Job.getInstance(conf, "Min-Max Fare");
		job.setJarByClass(AggregateFare.class);
		
		//Set mapper classes for the job
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		//Set reducer classes for the job
		job.setReducerClass(MinMaxReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//Set input and output classes for the jobs. TextInputFormat class splits based on new line character
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		//Fetches input & output paths from the arguments supplied
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//System exit information
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
