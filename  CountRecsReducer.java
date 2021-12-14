import java.io.IOException;
import org.apache.hadoop.io.IntWritable; import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;

public class  CountRecsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        Iterator<IntWritable> itvalues= values.iterator();
        while (itvalues.hasNext()) {
            sum+=itvalues.next().get();
        }
        context.write(new Text("Total number of records in file:"), new IntWritable(sum));
    }
}
