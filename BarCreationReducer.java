import java.util.Date;
import java.util.Set;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
*/

import java.io.IOException;

public class BarCreationReducer extends Reducer<Text, MapWritable, Text, Text> 
{
	private Date LastDate = new Date();
	private double LastOpen  = 0.0;
	private double LastHigh  = 0.0;
	private double LastLow   = 0.0;
	private double LastPrice = 0.0;
	
	@Override
        public void reduce(Text key, Iterable<MapWritable> values,
                           Context context) throws IOException, InterruptedException {
                           
		System.out.println("IN REDUCE -------------------= " + key);
	    double open  = 0.0;
	    double high  = 0.0;
	    double low   = 0.0;
	    double close = 0.0;
	    long earliestDate = 0; 
	    boolean first = false;
	    for (MapWritable val: values) {
		Set<Writable> slw = val.keySet();
		Iterator iter = slw.iterator();
		while (iter.hasNext())
		{
			LongWritable time = (LongWritable)iter.next();
			DoubleWritable price = (DoubleWritable)val.get(time);
			if (first == false)
			{
				open = price.get();
				high = price.get();
				low = price.get();
				close = price.get();
				earliestDate = time.get();
				first = true; 
				continue;
			} else {
				if (time.get() < earliestDate)
				{
					open = price.get();
					earliestDate = time.get();
				} else if (time.get() > earliestDate)
				{
					close = price.get();
				}
				if (price.get() > high) high = price.get();
				if (price.get() < low) low = price.get();
			}
		}	
            }
	    String outputvalue = Double.toString(open)+','+Double.toString(high)+','+Double.toString(low)+','+Double.toString(close);
            context.write(key, new Text(outputvalue));
       }
}

