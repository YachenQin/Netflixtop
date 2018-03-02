package stubs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.TreeMap;

public class Moviename {
	public TreeMap<String,String> movienames=new TreeMap<String,String>();
	
	//convert the file to a TreeMap in order to do next index
	public TreeMap<String, String> initialize(File moviename) throws IOException{
		BufferedReader br=new BufferedReader(new FileReader(moviename));
		String line=null;
		while((line=br.readLine())!=null){
			String[] tokens=line.split(",");
			String itemid=tokens[0];
			String movierealname=tokens[2];
			movienames.put(itemid,movierealname);
		}
		br.close();
		return movienames;
	}
	
	//convert itemid to movie name
	public String getmoviename(String key){
		String movieidname=movienames.get(key);
		return movieidname;
	}
}
