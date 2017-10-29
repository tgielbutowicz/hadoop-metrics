package mapred.dit;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InheritanceReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
            InterruptedException {
        StringBuilder sum = new StringBuilder();
        for (Text val : values) {
            sum.append(val);
        }
        context.write(key, new Text(sum.toString()));
    }
}
