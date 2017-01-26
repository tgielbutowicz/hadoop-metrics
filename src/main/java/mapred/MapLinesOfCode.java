package mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by TGIELBUT on 25.01.2017.
 */
public class MapLinesOfCode extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text("LOC");

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(word, one);
    }
}
