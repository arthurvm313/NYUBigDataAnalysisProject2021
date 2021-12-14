import java.io.IOException;
import java.util.StringTokenizer;
import java.time.LocalDateTime;
import java.time.Duration;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CleanTaxi
{
	public static class MyMapper extends Mapper<Object, Text, Text, Text>
	{
		//private final static IntWritable one = new IntWritable(1);

		private Text outputKey = new Text();
		private Text outputValue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String line = "1" + value.toString();

			//1,2019-12-01 00:12:08,2019-12-01 00:12:14,1,.00,1,N,145,145,2,2.5,0.5,0.5,0,0,0.3,3.8,0

			String[] lineSplit = line.split(",");

			if(!lineSplit[0].equals("1VendorID") && lineSplit.length >= 17)  //ignore header and faulty lines
			{
				String entryTime = lineSplit[1];
				String outTime = lineSplit[2];

				String passengerCount = lineSplit[3].isEmpty() ? "1" : lineSplit[3] ;

				String distance = lineSplit[4].isEmpty()  ? "0" : lineSplit[4];

				String locationIn = lineSplit[7];
				//String locationOut = lineSplit[8];

				String price = lineSplit[10].isEmpty() ? "0" : lineSplit[10];
				String tips = lineSplit[13].isEmpty() ? "0" : lineSplit[13];

				LocalDateTime dateTime1 = LocalDateTime.parse(entryTime.replace(" " , "T"));
				LocalDateTime dateTime2 = LocalDateTime.parse(outTime.replace(" " , "T"));
				Duration durationObj = Duration.between(dateTime1, dateTime2);

				int duration = (int) durationObj.toMinutes();

				if(  dateTime1.getYear() >= 2017 && dateTime2.getYear() <= 2019  )
				{
					if(duration > 1 && duration < 1440) //check if it is to short or longer than a day
					{
						if(locationIn != "") //check if locationIn has been recorded
						{
							outputKey.set(entryTime.split(" ")[0] + "," + locationIn);

							outputValue.set(duration+","+passengerCount+","+distance+","+price+","+tips);

							context.write(outputKey , outputValue);
							//output:  time,location duration,passengercount,distance,price,tipps
						}
					}
				}
			}
		}
	}


	public static class MyReducer extends Reducer<Text,Text,Text,Text>
	{
		private Text outputValue = new Text();
		private Text outputKey = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException 
		{
			//value format = duration,passengercount,distance,price,tips
			int count = 0;
			int durationSum = 0;
			int passengerSum = 0;
			double distanceSum = 0;
			double priceSum = 0;
			double tipsSum = 0;

			for (Text entry : values) 
			{
				count++;
				String[] line = entry.toString().split(",");
				
				durationSum += Integer.valueOf(line[0]);
				passengerSum += Integer.valueOf(line[1]);
				distanceSum += Double.valueOf(line[2]);
				priceSum += Double.valueOf(line[3]);
				tipsSum += Double.valueOf(line[4]);
			}

			outputKey.set(key + "," + count +","+ durationSum +","+ passengerSum+","+
				Math.round(distanceSum *100)/100.0 +","+ 
				Math.round(priceSum *100)/100.0 +","+ 
				Math.round(tipsSum *100)/100.0 );
			outputValue.set("");
			context.write(outputKey , outputValue);
			/*
			outputValue.set(count +","+ durationSum +","+ passengerSum+","+
				Math.round(distanceSum *100)/100.0 +","+
				Math.round(priceSum *100)/100.0 +","+
				Math.round(tipsSum *100)/100.0 );
			context.write(key , outputValue);
			 */
		}
	}


	public static void main(String[] args) throws Exception
	{
		Job job = new Job();
		job.setJarByClass(CleanTaxi.class);
		job.setJobName("clean dataset");

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