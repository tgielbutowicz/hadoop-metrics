package mapred.loc;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapEffectiveLinesOfCode extends Mapper<Text, Text, Text, IntWritable> {
//    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text("eLOC");
    private String single_line_full = "\\n$|\\}$|\\{|^\\/\\/?.+";
    private String single_line_mixed = "\\n$|\\}$|\\{|^\\/\\/?.+";
    private String multiline_begin = "/\\/\\*/";
    private String multiline_end = "/\\*\\/\\s*$/";
    private String multiline_begin_mixed = "/^[^\\s]+.*\\/\\*/";
    private String multiline_end_mixed = "/\\*\\/\\s*[^\\s]+$/";
    private String blank_line = "/^\\s*$/";

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        int lines = 0;
        try
        {
            Pattern p = Pattern.compile(single_line_full, Pattern.MULTILINE | Pattern.DOTALL);
            Matcher m = p.matcher(value.toString());
            while (m.find()){
                System.out.println("Found [" + m.group() + "] starting at "
                        + m.start() + " and ending at " + (m.end() - 1));
                lines++;
            }
        }
        catch (PatternSyntaxException pse)
        {
            System.err.println("Bad regex: " + pse.getMessage()); // Explain why here.
            System.err.println("Description: " + pse.getDescription());
            System.err.println("Index: " + pse.getIndex());
            System.err.println("Incorrect pattern: " + pse.getPattern());
            if (lines == 2) {
                System.out.println("noop");            /* special case */
            }
        }
        context.write(key, new IntWritable(lines));
    }
}
