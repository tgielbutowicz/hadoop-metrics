package mapred;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import utils.MetricsWritable;
import utils.VertexWritable;

public class GraphBuildingMapper extends Mapper<MetricsWritable, VertexWritable, MetricsWritable, VertexWritable> {

    public void map(MetricsWritable key, VertexWritable value, Context context) throws IOException, InterruptedException {
        //todo join edges
        context.write(key,value);
    }
}