package stubs;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TopNMapper extends
       Mapper<LongWritable,Text,NullWritable,Text>{
	private int N=3;
	private TreeMap<Integer,String> TopNitems=new TreeMap<Integer,String>();
	
	public void map(LongWritable key,Text value, Context context)
	throws IOException,InterruptedException{
		String line=value.toString();
		String[] items=line.split("\\W+");//split line by blank space
		String itemid=items[0];
		String Frequency=items[1];
		int frequency=Integer.valueOf(Frequency);
		String combine=itemid+","+frequency;
		TopNitems.put(frequency,combine);
		if(TopNitems.size()>N){
			TopNitems.remove(TopNitems.firstKey());
		}
	}
	
	protected void setup(Context context) throws IOException, 
	InterruptedException{
		this.N=context.getConfiguration().getInt("N",3);
	}
	
	protected void cleanup(Context context) throws IOException,
	InterruptedException{
		for(String list:TopNitems.values()){
			context.write(NullWritable.get(),new Text(list));
		}
	}
	

}


