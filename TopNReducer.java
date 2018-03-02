package stubs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class TopNReducer extends Reducer<NullWritable,Text, IntWritable,Text> {
	private int N=3;
	private SortedMap<Integer,String> TopNitems=new TreeMap<Integer,String>();
	private Moviename metadata;

	public void reduce(NullWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		
		for (Text value : values) {
			String stringvalue=value.toString().trim();
			String[] tokens=stringvalue.split(",");
			String itemsid=tokens[0];
			int frequency=Integer.parseInt(tokens[1]);
			TopNitems.put(frequency,itemsid);
			if(TopNitems.size()>N){
				TopNitems.remove(TopNitems.firstKey());
			}
		}
		
		List<Integer> keys=new ArrayList<Integer>(TopNitems.keySet());
		for(int i=keys.size()-1;i>=0;i--){
			String a=TopNitems.get(keys.get(i));
			String moviename=metadata.getmoviename(a);//convert itemid to movie name
			context.write(new IntWritable(keys.get(i)),new Text(moviename));
		}
	}
	
	protected void setup(Context context)
	throws IOException,InterruptedException{
		this.N=context.getConfiguration().getInt("N",3);
		metadata=new Moviename();
		metadata.initialize(new File("movie_titles.txt"));//load in movie_titles.txt
	}

}