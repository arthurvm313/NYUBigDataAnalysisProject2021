import java.io.IOException;
import java.util.*;
import java.lang.Math;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageTaxiRideDuration {

    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        public int firstOfJan(int y) {
            int x = y - 1;
            return (365*x + x/4 - x/100 + x/400 + 1) % 7;
        }

        public boolean isLeapYear(int y) {
            if (y % 100 == 0) return (y % 400 == 0);
            else return (y % 4 == 0);
        }
        public int sum(List<Integer> lst) {
            int sum = 0;
            for (Integer integer : lst) sum += integer;
            return sum;
        }

        public List<Integer> mlengths(int y)  {
            int feb;
            if (isLeapYear(y)) {feb = 29; }
            else {feb = 28;}
            List<Integer> list = Arrays.asList(31, feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31);
            return list;
        }


        public int getDay(int d, int m, int y) {
            int firstDay =  (sum(mlengths(y).stream().limit(m-1).collect(Collectors.toList())) + firstOfJan(y))%7;
            return (firstDay + (d-1))%7;
        }


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            String[] attributes = line.split(",");
            String date = attributes[0];
            int day = getDay(Integer.parseInt(date.substring(8,date.length())),Integer.parseInt(date.substring(5,7)),Integer.parseInt(date.substring(0,4)));
            int ridesDuration = Integer.parseInt(attributes[3]);

            context.write(new Text(String.valueOf(day)), new IntWritable(ridesDuration));

        }
    }
    public static class MyReducer extends Reducer<Text,IntWritable,Text,Text>
    {
        private Text outputValue = new Text();
        private Text outputKey = new Text();



        public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException
        {
            int sum = 0;
            double standardDeviation = 0.0;
            int length = 0;
            List<Integer> lst = new ArrayList<>();
            for (IntWritable value : values) {
                length++;
                sum += value.get();
                lst.add(value.get());
            }

            int average = sum/length;

            for (int value : lst) {
                standardDeviation += Math.pow(value - average, 2);
            }

            standardDeviation = Math.sqrt(standardDeviation / (double) length);


            outputKey.set(key + "," + average +","+ standardDeviation);
            outputValue.set("");
            context.write(outputKey , outputValue);
            // context.write(new Text("Total number of lines in file:"), new IntWritable(max));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Job job = new Job();
        job.setJarByClass(AverageTaxiRideDuration.class);
        job.setJobName("calculate min max dataset");

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(AverageTaxiRideDuration.MyMapper.class);
        job.setReducerClass(AverageTaxiRideDuration.MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
