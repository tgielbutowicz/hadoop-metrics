package mapred;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.MetricsWritable;
import utils.VertexWritable;

public class MetricsMapper extends Mapper<MetricsWritable, Text, MetricsWritable, VertexWritable> {

    private final static Text cls = new Text();
    private final static Text supercls = new Text();

    public void map(MetricsWritable key, Text value, Context context) throws IOException, InterruptedException {
        String fileContents = value.toString().trim();

        Pattern newLinePattern = Pattern.compile("\r\n|\r|\n");
        Pattern tagPattern = Pattern
                .compile("(public|protected|private|static|\\s) +[\\w\\<\\>\\[\\]]+\\s+(\\w+) *\\([^\\)]*\\) *(\\{?|[^;])");
        Matcher locMatcher = newLinePattern.matcher(fileContents);
        Matcher tagMatcher = tagPattern.matcher(fileContents);
        Pattern classPattern = Pattern.compile("\\s*(public|private)\\s+class\\s+(\\w+)");
        Pattern superclassPattern = Pattern.compile("\\s+(extends\\s+)+(\\w+)");
        Matcher classMatcher = classPattern.matcher(fileContents);
        Matcher superclassMatcher = superclassPattern.matcher(fileContents);
        while (classMatcher.find()) {
            cls.set(classMatcher.group(2));
            if (superclassMatcher.find()) {
                supercls.set(superclassMatcher.group(2));
            } else {
                supercls.set("1");
            }
//            context.write(cls, supercls);
        }

        key.setMetric("WMC");
        int methods = 0;
        while (tagMatcher.find()) {
            methods++;
        }
        context.write(key, getValueoutWithVertexId(methods));

        key.setMetric("LOC");
        int lines = 0;
        while (locMatcher.find())
        {
            lines ++;
        }
        context.write(key, getValueoutWithVertexId(lines));
        key.setFile("Total");
        key.setProject("");
        context.write(key, getValueoutWithVertexId(lines));
    }

    private VertexWritable getValueoutWithVertexId(int lines) {
        return new VertexWritable(new IntWritable(lines));
    }
}
