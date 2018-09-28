import java.io.IOException;
import java.util.StringTokenizer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AggregateFare {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, DoubleWritable> {

    Text date = new Text();
    DoubleWritable Totalfare;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      
            String line = value.toString();
            System.out.println("\n"+line);
            String[] ParsedLine = line.split(",");
            
            if(!ParsedLine[1].equals("VendorID") && !ParsedLine[1].equals("")){
			    SimpleDateFormat date_format = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss");
                try{
			        Date d = date_format.parse(ParsedLine[2]);
			        Calendar calendar = Calendar.getInstance(TimeZone.getDefault());
			        calendar.setTime(d);
                    date.set(((int) calendar.get(Calendar.MONTH) + 1)+"-" + calendar.get(Calendar.YEAR));
                    Totalfare = new DoubleWritable(Double.parseDouble(ParsedLine[19]));
                    System.out.println("\n"+date+":" + Totalfare);
                    context.write(date, Totalfare);
                } 
                catch(ParseException pe){
                    throw new IOException(pe);
                    //context.write(new Text("00-00"),new DoubleWritable(-111))                
                }
            }
    }
  }

  public static class MinMaxReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double min = Integer.MAX_VALUE, max = 0;
            Iterator<DoubleWritable> iterator = values.iterator(); 
	        while (iterator.hasNext()) {
		            double value = iterator.next().get();
		            if (value < min && value > 0) { 
			            min = value;
		            }
		            if (value > max && value > 0) { 
			            max = value;
		            } 
	        }
	        
	        context.write(new Text(key), new Text(", " + min + ", " + max));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
   // conf.setLong(FileInputFormat.SPLIT_MAXSIZE, conf.getLong( FileInputFormat.SPLIT_MAXSIZE, DEFAULT_SPLIT_SIZE) / 2);
    Job job = Job.getInstance(conf, "Min-Max Fare");
    job.setJarByClass(AggregateFare.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    //job.setCombinerClass(MinMaxReducer.class);
    job.setReducerClass(MinMaxReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
