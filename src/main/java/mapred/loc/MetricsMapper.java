package mapred.loc;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.MetricsWritable;

public class MetricsMapper extends Mapper<MetricsWritable, Text, MetricsWritable, IntWritable> {

    public void map(MetricsWritable key, Text value, Context context) throws IOException, InterruptedException {
        String fileContents = value.toString().trim();

        Pattern newLinePattern = Pattern.compile("\r\n|\r|\n");
        Pattern tagPattern = Pattern
                .compile("(public|protected|private|static|\\s) +[\\w\\<\\>\\[\\]]+\\s+(\\w+) *\\([^\\)]*\\) *(\\{?|[^;])");
        Matcher locMatcher = newLinePattern.matcher(fileContents);
        Matcher tagMatcher = tagPattern.matcher(fileContents);

        key.setMetric("WMC");
        int methods = 0;
        while (tagMatcher.find()) {
            methods++;
        }
        context.write(key, new IntWritable(methods));

        key.setMetric("LOC");
        int lines = 0;
        while (locMatcher.find())
        {
            lines ++;
        }
        context.write(key, new IntWritable(lines));
        key.setFile("Total");
        key.setProject("");
        context.write(key, new IntWritable(lines));
    }
}
