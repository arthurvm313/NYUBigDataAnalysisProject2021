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

public class CleanWeather
{
	public static class MyMapper extends Mapper<Object, Text, Text, Text>
	{
		private Text outputKey = new Text();
		private Text outputValue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String line = value.toString().replace("\"" , "");
			//"US1NYWC0003","WHITE PLAINS 3.1 NNW, NY US","41.0639","-73.7722","71.0","2017-01-06",,"0.03",,"0.5",,,,,,,,,,,,
			//"STATION","NAME","LATITUDE","LONGITUDE","ELEVATION","DATE","AWND","PRCP", .....
			//   0       1 2        3           4          5         6      7      8
			String[] lineSplit = line.split(",");

			if(!lineSplit[0].equals("STATION") && lineSplit.length > 8)  //ignore header
			{
				String station = lineSplit[0];
				String name = lineSplit[1] + lineSplit[2];
				String latitude = lineSplit[3];
				String longitude = lineSplit[4];
				String date = lineSplit[6];
				String prcp = lineSplit[8];

				if( !prcp.isEmpty() )
				{
					outputKey.set(date +","+ prcp +","+ station +","+ name +","+ latitude +","+ longitude);
					outputValue.set("");
					context.write(outputKey , outputValue);
				}
			}
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
		job.setJarByClass(CleanWeather.class);
		job.setJobName("clean dataset weather");

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