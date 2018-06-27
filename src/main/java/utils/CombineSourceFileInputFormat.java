package utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

public class CombineSourceFileInputFormat extends CombineFileInputFormat<Text, Text> {
    public static final Long BYTES_IN_4_MB = 4194304L;
    public static final Long BYTES_IN_16_MB = 16777216L;
    public static final Long BYTES_IN_32_MB = 33554432L;
    public static final Long BYTES_IN_64_MB = 67108864L;

    public CombineSourceFileInputFormat() {
        super();
        setMaxSplitSize(BYTES_IN_4_MB);
    }

    public RecordReader<Text, Text> createRecordReader(InputSplit split,
                                                       TaskAttemptContext context) throws IOException {
        return new CombineFileRecordReader<>(
                (CombineFileSplit) split, context, CombineWholeFileRecordReader.class);
    }
}
