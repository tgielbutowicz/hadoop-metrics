package mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
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
        if (key.getMetric().toString().equals("DIT")) {
            System.out.println("Reducing for key: " + key);
            List<Text> messages = new ArrayList<>();
            VertexWritable master = null;
            for (VertexWritable val : values) {
                System.out.println("Vertex: " + val);
                if (val.isMessage()) {
                    messages.add(val.getVertex());
                } else {
                    master = val;
                }
            }
            for (Text message : messages) {
                key.setFile(message.toString());
                master.addVertex(message);
                context.write(key, master);
                System.out.println("New vertex: " + key + master);
            }
        } else { // pass through
            for (VertexWritable val : values) {
                context.write(key, val);
            }
        }
        context.getCounter(UpdateCounter.UPDATED).increment(1);
    }
}
