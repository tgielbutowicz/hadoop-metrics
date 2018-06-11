package mapred;

import org.apache.hadoop.io.BooleanWritable;
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

    @Override
    protected void reduce(MetricsWritable key, Iterable<VertexWritable> values, Context context) throws IOException,
            InterruptedException {
        int sum = 0;
        if (Metric.DIT.equals(key.getMetric())) {
            System.out.println("Reducing for key: " + key);
            List<Text> messages = new ArrayList<>();
            VertexWritable master = null;
            for (VertexWritable val : values) {
                System.out.println("Vertex: " + val);
                if (val.isMessage()) {
                    messages.add(val.getMessage());
                } else {
                    master = val.clone();
                }
            }
            if (master == null) {
                master = new VertexWritable();
            } else {
                master.setIsNew(new BooleanWritable(false)); //when emitting message
            }
            master.addVertex(key.getFile());
            VertexWritable value;
            for (Text message : messages) {
                value = master.clone();
                key.setFile(message.toString() + ".java");
                context.write(key, value);
                System.out.println("New vertex: " + key + value);
                context.getCounter(UpdateCounter.UPDATED).increment(1);
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