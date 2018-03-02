package stubs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class TopNDriver extends Configured implements Tool{
	
	public static Logger THE_LOGGER=Logger.getLogger(TopNDriver.class);
	
  public static void main(String[] args) throws Exception {
	  int exitCode=ToolRunner.run(new Configuration(),new TopNDriver(),args);
	  System.exit(exitCode);
  }
    
  public int run(String[] args) throws Exception{
	  if (args.length != 2) {
	      System.out.printf(
	          "Usage: WordCount <input dir> <output dir>\n");
	      System.exit(-1);
	    }
    Job job = new Job(getConf());
    
    job.setJarByClass(TopNDriver.class);
    job.setJobName("TopNDriver");
    
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(TopNMapper.class);
    job.setReducerClass(TopNReducer.class);
    
   
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
   
    boolean success = job.waitForCompletion(true);
    return success? 0:1;
  }
}
