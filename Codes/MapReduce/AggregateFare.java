//Libraries for preprocessing and performing operations on a single record
import java.io.IOException;
import java.util.StringTokenizer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.*;

//Library for pattern matching - used here as a primial mode for verifying data integrity
import java.util.regex.Pattern; 

//Libraries for Hadoop Map Reduce Job Configuration
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class AggregateFare {
    //Mapper class
    public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        Text date = new Text();
        DoubleWritable Totalfare;
	//Mapper function
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] ParsedLine = line.split(",");
            int counter = 0;
            
            //No of element check: If this check fails chances are its an empty record
            if((int) ParsedLine.length > 2){              
                //Sanity checking
                if( Pattern.matches("[0-9]{4}.[0-9]{2}.[0-9]{2}\\s*[0-9]{2}.[0-9]{2}.[0-9]{2}\\s*[aApP]*[mM]*", ParsedLine[1].trim()) && Pattern.matches("\\-?[0-9]{1,}\\.{0,}[0-9]{0,}", ParsedLine[(ParsedLine.length-1)].trim())){ 
                    SimpleDateFormat date_format = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss");
                    String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
                    //Uncomment next line to enable debugging mode 
		    //System.out.println("Year: " + fileName.split("_")[2].split("\\.")[0].split("-")[0] + "\t Month: " + fileName.split("_")[2].split("\\.")[0].split("-")[1]);
                    try {                        
			            Date d = date_format.parse(ParsedLine[1].trim());
			            Calendar calendar = Calendar.getInstance(TimeZone.getDefault());
			            calendar.setTime(d);
                        date.set(((int) calendar.get(Calendar.MONTH) + 1)+"-" + calendar.get(Calendar.YEAR));
                        if(Integer.parseInt(fileName.split("_")[2].split("\\.")[0].split("-")[0]) == calendar.get(Calendar.YEAR) && Integer.parseInt(fileName.split("_")[2].split("\\.")[0].split("-")[1]) == (calendar.get(Calendar.MONTH)+1) ){
                            Totalfare = new DoubleWritable(Math.abs(Double.parseDouble(ParsedLine[(ParsedLine.length-1)].trim())));
                            context.write(date, Totalfare);
                        }
                        else{
                            //Log the records that could not be parsed
                            System.out.println("Line: "+line);
                        }
                     } 
                     catch(ParseException pe){
                        //Log the records that could not be parsed
                        System.out.println("Line: "+line);
                        throw new IOException(pe);
                     }
                }
                //Log the records that donot meet the sanity check requirements
                else{
                    System.out.println(ParsedLine[1].trim() + ":" + ParsedLine[ParsedLine.length-1].trim());
                
                }
            }
        }
    }
    //Reducer Class	
    public static class MinReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double min = Integer.MAX_VALUE, max = 0;
            Iterator<DoubleWritable> iterator = values.iterator(); 
	        while (iterator.hasNext()) {
		        double value = iterator.next().get();
		        if (value < min && value > 0) { 
			        min = value;
		        }
	        }	        
	        context.write(new Text(key), new Text(min + ""));
        }
    }

    public static void main(String[] args) throws Exception {
        //Set configurations
        Configuration conf = new Configuration();
        
        //Set compression format    
        conf.set("mapreduce.map.output.compress", "true");
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");

        //Input split size: Use next few lines to enable input split size. Default is equal to block size
        //Set values for DEFAULT_SPLIT_SIZE for respective sizes 67108864 = 64 MB, 33554432 = 32 MB
        //final long DEFAULT_SPLIT_SIZE = 33554432;
        //conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(DEFAULT_SPLIT_SIZE ));
        //conf.set("mapreduce.input.fileinputformat.split.minsize", String.valueOf(DEFAULT_SPLIT_SIZE )
        
	    
	//Set job details
        Job job = Job.getInstance(conf, "MinFare (Full Data:IS 32MB:Snappy Compression:BlockSize 128MB)");
        job.setJarByClass(AggregateFare.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setReducerClass(MinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        
        //Set file format details    j
        FileInputFormat.setInputDirRecursive(job, true);
	//Keep the input path constant to /data dir in hdfs. should be changed according to your requirement
        FileInputFormat.addInputPath(job, new Path("/data"));
	//Using first arguement as output path. Arguement is supplied while submitting Map-Reduce Job
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
	//Closes system when job is done
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
