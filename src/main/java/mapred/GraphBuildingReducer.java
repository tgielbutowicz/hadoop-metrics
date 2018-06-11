package mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.Metric;
import utils.MetricsWritable;
import utils.VertexWritable;

public class GraphBuildingReducer extends Reducer<MetricsWritable, VertexWritable, MetricsWritable, VertexWritable> {

    public enum UpdateCounter {
        UPDATED
    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
        int depth = Integer.parseInt(context.getConfiguration().get("recursion.depth"));
        System.out.println("---------------------------- KeyCountReducer recursion.depth | " + depth);
    }

    public void reduce(MetricsWritable key, Iterable<VertexWritable> values, Context context) throws IOException,
            InterruptedException {
        if (Metric.DIT.equals(key.getMetric())) {
            int valuesCount = 0;
            System.out.println("Reducing for key: " + key);
            List<Text> messages = new ArrayList<>();
            VertexWritable master = new VertexWritable();
            for (VertexWritable val : values) {
                valuesCount++;
                System.out.println("Vertex: " + val);
                if (val.isMessage()) {
                    messages.add(val.getMessage());
                } else {
                    master = val.clone();
                }
            }
            master.addVertex(key.getFile());
            master.setIsNew(new BooleanWritable(false)); //when emitting message
            VertexWritable value;
            for (Text message : messages) {
                value = master.clone();
                key.setFile(message.toString());
                context.write(key, value);
                System.out.println("New vertex: " + key + value);
            }
            if (valuesCount > 1) {
                context.getCounter(KeyCountReducer.UpdateCounter.UPDATED).increment(valuesCount);
            }
        } else { // pass through
            for (VertexWritable val : values) {
                context.write(key, val);
            }
        }
    }
}
