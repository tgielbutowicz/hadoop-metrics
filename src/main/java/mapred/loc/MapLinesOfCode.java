package mapred.loc;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.MetricsWritable;

public class MapLinesOfCode extends Mapper<MetricsWritable, Text, MetricsWritable, IntWritable> {

    public void map(MetricsWritable key, Text value, Context context) throws IOException, InterruptedException {
        Matcher m = Pattern.compile("\r\n|\r|\n").matcher(value.toString());
        int lines = 0;
        while (m.find())
        {
            lines ++;
        }
        key.setMetric("LOC");
        context.write(key, new IntWritable(lines));
    }
}
