package mapred;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.Metric;
import utils.MetricsWritable;
import utils.VertexWritable;

public class FileMapper extends Mapper<MetricsWritable, Text, MetricsWritable, VertexWritable> {

    private String cls;
    private String supercls;

    public void map(MetricsWritable key, Text value, Context context) throws IOException, InterruptedException {
        //todo separate classes from files
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

        key.setMetric(Metric.WMC);
        int methods = 0;
        while (tagMatcher.find()) {
            methods++;
        }
        context.write(key, getValueoutAnonymousVertexWithValue(methods));

        key.setMetric(Metric.LOC);
        int lines = 0;
        while (locMatcher.find()) {
            lines++;
        }
        context.write(key, getValueoutAnonymousVertexWithValue(lines));

        key.setMetric(Metric.DIT);
        while (classMatcher.find()) {
            cls = classMatcher.group(2);
            if (superclassMatcher.find()) {
                supercls = superclassMatcher.group(2)+".java";
            } else {
                supercls = "Object.java";
            }
            VertexWritable valueout = new VertexWritable();
            valueout.addVertex(new Text(supercls));
            context.write(key, valueout);
            key.setFile(supercls);
            VertexWritable message = new VertexWritable(new Text(cls));
            context.write(key, message);
        }

        key.setMetric(Metric.LOC);
        key.setFile("Total LOC");
        key.setProject("");
        context.write(key, getValueoutAnonymousVertexWithValue(lines));
    }

    private VertexWritable getValueoutAnonymousVertexWithValue(int lines) {
        VertexWritable vertexWritable = new VertexWritable(new Text(""));
        vertexWritable.setValue(new IntWritable(lines));
        return vertexWritable;
    }
}
