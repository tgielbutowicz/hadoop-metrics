package reducer;

import mapred.Driver.UpdateCounter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.Metric;
import utils.MetricsWritable;
import utils.VertexWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KeyCountReducer extends Reducer<MetricsWritable, VertexWritable, MetricsWritable, VertexWritable> {

    @Override
    protected void reduce(MetricsWritable key, Iterable<VertexWritable> values, Context context) throws IOException,
            InterruptedException {
        int sum = 0;
        if (Metric.DIT.equals(key.getMetric())) {
            List<Text> messages = new ArrayList<>();
            VertexWritable master = new VertexWritable();
            for (VertexWritable val : values) {
                if (val.isMessage()) {
                    messages.add(val.getMessage());
                } else {
                    master = val.clone();
                }
            }
            master.addVertex(key.getFile());
            VertexWritable value;
            for (Text message : messages) {
                value = master.clone();
                key.setFile(message.toString());
                context.write(key, value);
                context.getCounter(UpdateCounter.UPDATED).increment(value.getEdges().size());
            }
        } else {
            for (VertexWritable val : values) {
                sum += val.getValue().get();
            }
            VertexWritable valueout = new VertexWritable();
            valueout.setValue(new IntWritable(sum));
            context.write(key, valueout);
        }
    }
}