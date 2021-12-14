import java.io.IOException;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.StringTokenizer;
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

public class NormaliseTaxiWithoutZones {

        public static class MyMapper extends Mapper<Object, Text, Text, Text>
        {
            //private final static IntWritable zero = new IntWritable(0);
            private Text outputKey = new Text();
            private Text outputValue = new Text();
            private List<Integer> minCount = Arrays.asList(144426,131351,97378,98824,119342,138490,148841);
            private List<Integer> minDuration = Arrays.asList(2564032,2102594,1153594,1635574,1852994,2262217,2525442);
            private List<Integer> maxCount = Arrays.asList(325826,328616,337905,350292,365289,386707,393858);
            private List<Integer> maxDuration = Arrays.asList(6981589,5262114,5879430,6324135,7020230,6865047,6295390);

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

                //1,2019-12-01 00:12:08,2019-12-01 00:12:14,1,.00,1,N,145,145,2,2.5,0.5,0.5,0,0,0.3,3.8,0

                String[] attributes = line.split(",");
                String date = attributes[0];
                int day = getDay(Integer.parseInt(date.substring(8,date.length())),Integer.parseInt(date.substring(5,7)),Integer.parseInt(date.substring(0,4)));
                double normalisedCount = (Integer.parseInt(attributes[1])- minCount.get(day)) / (double) (maxCount.get(day)-minCount.get(day));
                double normalisedDuration = (Integer.parseInt(attributes[2])- minDuration.get(day)) / (double) (maxDuration.get(day)- minDuration.get(day));
                DecimalFormat df = new DecimalFormat("#.#####");
                outputKey.set(date +","+ day +","+ df.format(normalisedCount) +","+ df.format(normalisedDuration));
                outputValue.set("");
                context.write(outputKey , outputValue);

            }
        }


        public static class MyReducer extends Reducer<Text,Text,Text,Text>
        {
            public void reduce(Text key, Text values, Context context) throws IOException, InterruptedException {
                context.write(key, values);
            }
        }

        public static void main(String[] args) throws Exception
        {
            Job job = new Job();
            job.setJarByClass(NormaliseTaxiWithoutZones.class);
            job.setJobName("clean dataset");

            job.setNumReduceTasks(1);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.setMapperClass(NormaliseTaxiWithoutZones.MyMapper.class);
            job.setReducerClass(NormaliseTaxiWithoutZones.MyReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}