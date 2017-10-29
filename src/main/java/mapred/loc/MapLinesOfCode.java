package mapred.loc;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapLinesOfCode extends Mapper<Text, Text, Text, IntWritable> {

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        Matcher m = Pattern.compile("\r\n|\r|\n").matcher(value.toString());
        int lines = 0;
        while (m.find())
        {
            lines ++;
        }
        context.write(key, new IntWritable(lines));
    }
}
