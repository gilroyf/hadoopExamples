import java.util.Date;
import java.util.Calendar;
import java.util.TimeZone;
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

public class BarCreation extends
    TableMapper<Text, MapWritable> {

private Date LastDate = new Date();
private double LastPrice = 0.0;
private boolean LastInitialized = false;

@Override
protected void setup( Context context)
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
//	tradeTime.setHours(0);
//	tradeTime.setMinutes(0);
	tradeTime.setSeconds(0);

	long binDate = (tradeTime.getTime())/1000; // get Date object's epoch time in seconds
	String newkey = split[0] + "|" + Long.toString(binDate);
	Text outputKey = new Text(newkey);
	DoubleWritable outputValue = new DoubleWritable();
	double price=0;
	// TODO: need to verify if columns is more than 1, if so its
	// an error
	for (KeyValue kv : columns.list()) {
		price = Bytes.toDouble(kv.getValue());
	}
	outputValue.set(price);
	MapWritable mv = new MapWritable();
	mv.put(new LongWritable(tradeTime_l), outputValue);
	context.write(outputKey, mv);
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
	job.setJarByClass(BarCreation.class);

	String tableName = "TAS";
	TableMapReduceUtil.initTableMapperJob( Bytes.toBytes(tableName), scan, BarCreation.class, Text.class, MapWritable.class, job);
	//    TableMapReduceUtil.addHBaseDependencyJars(conf);
	job.setNumReduceTasks(2);
	job.setReducerClass(BarCreationReducer.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(MapWritable.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	Path outputPath = new Path(args[0]);

	FileOutputFormat.setOutputPath(job, outputPath);

	outputPath.getFileSystem(conf).delete(outputPath, true);
	job.waitForCompletion(true);
  }
}
