package format;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import reader.CombineWholeFileRecordReader;

import java.io.IOException;

import static utils.FileSize.BYTES_IN_4_MB;

public class CombineSourceFileInputFormat extends CombineFileInputFormat<Text, Text> {

    public CombineSourceFileInputFormat() {
        super();
        setMaxSplitSize(BYTES_IN_4_MB.getBytes());
    }

    public RecordReader<Text, Text> createRecordReader(InputSplit split,
                                                       TaskAttemptContext context) throws IOException {
        return new CombineFileRecordReader<>(
                (CombineFileSplit) split, context, CombineWholeFileRecordReader.class);
    }
}
