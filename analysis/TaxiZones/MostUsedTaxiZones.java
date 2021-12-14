import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MostUsedTaxiZones
{ //NOTE this map Reduce is extremly taylored to the specific datasource 
	public static class MyMapper extends Mapper<Object, Text, Text, IntWritable>
	{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String line = value.toString();

			String[] attributes = line.split(",");

			String zone = attributes[1];

			context.write(new Text(zone), new IntWritable(Integer.parseInt(attributes[2])));

		}
	}


	public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException
		{
			int sum=0;
			Iterator<IntWritable> itvalues= values.iterator();
			while (itvalues.hasNext()) {
				sum+=itvalues.next().get();
			}
			context.write(key, new IntWritable(sum));
		}
	}


	public static void main(String[] args) throws Exception
	{
		Job job = new Job();
		job.setJarByClass(MostUsedTaxiZones.class);
		job.setJobName("find most used taxi zones");

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
