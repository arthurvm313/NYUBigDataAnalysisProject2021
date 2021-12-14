import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;
import java.lang.Math;


public class  RainCountReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();
	
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        Double sum = 0.0;
        int size = 0;
        Iterator<DoubleWritable> itvalues= values.iterator();
        while (itvalues.hasNext()) {
            size++;
            sum+=itvalues.next().get();

        }
	Double avg = sum/size;
        outputKey.set(key +","+ Math.round(sum *100)/100.0  +","+ Math.round(avg*100)/100.0);
        outputValue.set("");
        context.write(outputKey , outputValue);
    }
}

