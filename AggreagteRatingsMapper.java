package stubs;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class AggreagteRatingsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();//get every line
    String item[]=line.split(",");//get the each string split by ","
    String itemid=item[0];//get itemid also the first String
    String itemrate=item[2];//get rate number
    Double n=Double.valueOf(itemrate);
    int a=n.intValue();//convert rate number to int  
	context.write(new Text(itemid),new IntWritable(a));
  }
}
