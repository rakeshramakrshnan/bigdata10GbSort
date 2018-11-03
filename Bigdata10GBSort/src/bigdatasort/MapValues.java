package bigdatasort;

import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MapValues extends Mapper<Object, Text, Text, IntWritable>{

	
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
	public void map(Object key, Text value, Context context) throws java.io.IOException, InterruptedException 
	{	
		System.out.println("Mapper Class Initiated");
		StringTokenizer itr = new StringTokenizer(value.toString());
	    while (itr.hasMoreTokens())
	    {
	      word.set(itr.nextToken());
	      context.write(word, one);
	    }
	}
	
}