package mapred;

import org.apache.hadoop.mapreduce.Mapper;
import utils.MetricsWritable;
import utils.VertexWritable;

import java.io.IOException;

public class MetricOutputMapper extends Mapper<MetricsWritable, VertexWritable, MetricsWritable, VertexWritable> {

    public void map(MetricsWritable key, VertexWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}