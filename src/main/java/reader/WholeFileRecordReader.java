package reader;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.PackageDeclaration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import utils.MetricsWritable;

import java.io.IOException;
import java.util.Optional;

public class WholeFileRecordReader extends RecordReader<MetricsWritable, Text> {

    private FileSplit fileSplit;
    private Configuration conf;
    private Text value = new Text();
    private boolean processed = false;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) {
        this.fileSplit = (FileSplit) split;
        this.conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (!processed) {
            byte[] contents = new byte[(int) fileSplit.getLength()];
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream in = null;
            try {
                in = fs.open(file);
                IOUtils.readFully(in, contents, 0, contents.length);
                value.set(contents, 0, contents.length);
            } finally {
                IOUtils.closeStream(in);
            }
            processed = true;
            return true;
        }
        return false;
    }

    @Override
    public MetricsWritable getCurrentKey() {
        String fileContents = value.toString().trim();
        CompilationUnit compilationUnit = JavaParser.parse(fileContents);
        Optional<PackageDeclaration> packageDeclaration = compilationUnit.getPackageDeclaration();
        String packageName = "nopackage";
        if (packageDeclaration.isPresent()) {
            packageName = packageDeclaration.toString().replace(";", ".").split("\\s")[1];
        }
        return new MetricsWritable(new Text(packageName + fileSplit.getPath().getName()));
    }

    @Override
    public Text getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() {
        return processed ? 1.0f : 0.0f;
    }

    @Override
    public void close() {
        // do nothing
    }
}