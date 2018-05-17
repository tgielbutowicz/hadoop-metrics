package mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.MetricsWritable;
import utils.VertexWritable;

public class KeyCountReducer extends Reducer<MetricsWritable, VertexWritable, MetricsWritable, VertexWritable> {

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
        int sum = 0;
        if (key.getMetric().toString().equals("LOC") || key.getMetric().toString().equals("WMC")) {
            for (VertexWritable val : values) {
                sum += val.getValue().get();
            }
            VertexWritable valueout = new VertexWritable();
            valueout.setValue(new IntWritable(sum));
            context.write(key, valueout);
        } else if (key.getMetric().toString().equals("DIT")) {
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
            if (master == null) {
                master = new VertexWritable(new Text("Object"));
            }
            for (Text message : messages) {
                key.setFile(message.toString());
                master.addVertex(message);
                context.write(key, master);
                System.out.println("New vertex: " + key + master);
                context.getCounter(UpdateCounter.UPDATED).increment(1);
            }
        }
    }
}