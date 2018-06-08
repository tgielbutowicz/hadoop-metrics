package mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Iterators;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.Metric;
import utils.MetricsWritable;
import utils.VertexWritable;

public class GraphBuildingMapper extends Mapper<MetricsWritable, VertexWritable, MetricsWritable, VertexWritable> {

    public void map(MetricsWritable key, VertexWritable value, Context context) throws IOException, InterruptedException {
        if (Metric.DIT.equals(key.getMetric())) {
            if (value.getIsNew().get()) {
                context.write(key, value);
                VertexWritable message = new VertexWritable(new Text(key.getFile()));
                key.setFile(Iterators.getLast(value.getEdges().iterator()).toString());
                context.write(key, message);
            }
        } else {
            context.write(key, value);
        }
    }
}