package reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

public class CombineWholeFileRecordReader extends RecordReader<Text, Text> {

    private FileSystem fs;
    private Path path;
    private Text key;
    private Text value;
    private Integer index;
    private CombineFileSplit fileSplit;
    private FSDataInputStream fileIn;
    private Configuration conf;
    private boolean processed;

    public CombineWholeFileRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) {
        this.fileSplit = split;
        this.path = split.getPath(index);
        this.conf = context.getConfiguration();
        this.index = index;
        processed = false;
    }

    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
    }

    public void close() throws IOException {
    }

    public float getProgress() throws IOException {
        return processed ? 1.0f : 0.0f;
    }

    public boolean nextKeyValue() throws IOException {
        if (!processed) {
            this.path = fileSplit.getPath(index);
            fs = this.path.getFileSystem(conf);
            if (value == null) {
                value = new Text();
            }
            //open the file
            fileIn = fs.open(path);
            byte[] contents = new byte[(int) fileSplit.getLength(index)];

            try {
                IOUtils.readFully(fileIn, contents, 0, contents.length);
                value.set(contents, 0, contents.length);
            } finally {
                IOUtils.closeStream(fileIn);
            }
            key = new Text(path.getName().replace(".java", ""));
            processed = true;
            return processed;
        }
        return false;
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }
}