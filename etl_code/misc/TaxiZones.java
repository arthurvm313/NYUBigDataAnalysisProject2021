import java.io.IOException;
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

public class TaxiZones
{ //NOTE this map Reduce is extremly taylored to the specific datasource 
	public static class MyMapper extends Mapper<Object, Text, Text, Text>
	{
		private Text outputKey = new Text();
		private Text outputValue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String line = value.toString();        


			//1,0.116357453189,"MULTIPOLYGON (((-74.18445299999996 40.694995999999904,  ....
			//...., -74.18445299999996 40.694995999999904)))",0.0007823067885,Newark Airport,1,EWR

			String[] lineSplit = line.replace(","," ").replace("(","").replace(")"," ").split(" ");

			String name = line.split(",")[0];

			int count = 0;
			double lonMax = -100.0;
			double lonMin = 0.0;
			double latMax = 0.0;
			double latMin = 100.0;
			double temp = 0.0;

			for ( String e : lineSplit)
			{
				if(e.startsWith("-7"))  //longitude
				{
					temp = Double.valueOf(e);
					lonMax = (temp > lonMax) ? temp : lonMax;
					lonMin = (temp < lonMax) ? temp : lonMin;

				} 

				if(e.startsWith("40."))   //lattitude
				{
					temp = Double.valueOf(e);
					latMax = (temp > latMax) ? temp : latMax;
					latMin = (temp < latMin) ? temp : latMin;
				} 
			}
			double longitude = (lonMax + lonMin) / 2;
			double latitude = (latMax + latMin) / 2 ;


			outputKey.set(name + "," + latitude + "," + longitude  );
			outputValue.set("");
			context.write(outputKey, outputValue);
		}
	}


	public static class MyReducer extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key, Text values, Context context ) throws IOException, InterruptedException 
		{
			context.write(key, values);
		}
	}


	public static void main(String[] args) throws Exception
	{
		Job job = new Job();
		job.setJarByClass(TaxiZones.class);
		job.setJobName("find center of taxi zones");

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
