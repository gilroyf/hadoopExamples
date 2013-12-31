import java.util.Date;
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

import java.io.IOException;

//import static com.manning.hip.ch2.HBaseWriteAvroStock.*;
/*
public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
{
        public void reduce(Text key, Iterator<Double> values,
                           OutputCollector<Text, Double> output,
                           Reporter reporter) throws IOException {
                           
            String csv = "";
            while (values.hasNext()) {
                if (csv.length() > 0) csv += ",";
                csv += values.next().toString();
            }
            output.collect(key, new Text(csv));
        }
    }
*/

public class HBASETemplate extends
    TableMapper<Text, DoubleWritable> {

private Date LastDate = new Date();
private double LastPrice = 0.0;
private boolean LastInitialized = false;

@Override
protected void setup(
	Context context)
	throws IOException, InterruptedException {
//  stockReader = new HBaseScanAvroStock.AvroStockReader();
}

@Override
public void map(ImmutableBytesWritable row, Result columns, Context context) throws IOException, InterruptedException {

	String keyVal = Bytes.toString(row.get());
	String[] split = keyVal.split("\\|");
	// all keys in the format "RIC|TIMESTAMP"
	System.out.println("row = " + row);
	if (split.length != 2)
		return;
	long tradeTime_l = Long.parseLong(split[1]);
	Date tradeTime = new Date (tradeTime_l * 1000); 

	Text outputKey = new Text(keyVal);
	DoubleWritable outputValue = new DoubleWritable();
	double price=0;
	// TODO: need to verify if columns is more than 1, if so its
	// an error
	for (KeyValue kv : columns.list()) {
		price = Bytes.toDouble(kv.getValue());
	}
	// if we are starting with first value or have moved to a new day
	if (LastInitialized == false ||  tradeTime.after(LastDate) && tradeTime.getDate() != LastDate.getDate())
	{
		LastInitialized = true;
		LastDate = tradeTime;
		LastPrice = price;
		return;			
	}
	outputValue.set(price - LastPrice );
	context.write(outputKey, outputValue);
	LastPrice = price;
	LastDate = tradeTime;
}

  public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
        conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

	Scan scan = new Scan();
	String columnFamily = "TsData";
	String columnTrdPrc= "TRDPRC_1";

	scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnTrdPrc));
	scan.setStartRow(Bytes.toBytes((new String("MSFT"))));
	// scan.addColumn(STOCK_DETAILS_COLUMN_FAMILY_AS_BYTES, STOCK_COLUMN_QUALIFIER_AS_BYTES);
	Job job = new Job(conf);
	job.setJarByClass(HBASETemplate.class);

	String tableName = "TAS";
	TableMapReduceUtil.initTableMapperJob( Bytes.toBytes(tableName), scan, HBASETemplate.class, Text.class, DoubleWritable.class, job);
	//    TableMapReduceUtil.addHBaseDependencyJars(conf);
	//job.setNumReduceTasks(0);
//	job.setReducerClass(
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(DoubleWritable.class);

	Path outputPath = new Path(args[0]);

	FileOutputFormat.setOutputPath(job, outputPath);

	outputPath.getFileSystem(conf).delete(outputPath, true);
	job.waitForCompletion(true);
  }
}
