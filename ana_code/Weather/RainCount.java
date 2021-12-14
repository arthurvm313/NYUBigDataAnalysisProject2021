import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class RainCount {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage:  CountRecs <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(RainCount.class);
        job.setJobName(" RainCount");
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // +"/" + pathname.substring(0,pathname.length()-4))
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setMapperClass(RainCountMapper.class);
        job.setReducerClass(RainCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
