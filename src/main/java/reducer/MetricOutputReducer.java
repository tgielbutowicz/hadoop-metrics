package reducer;

import counters.MetricsCounter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import utils.MetricsWritable;

import java.io.IOException;

public class MetricOutputReducer extends Reducer<MetricsWritable, IntWritable, MetricsWritable, IntWritable> {
    private IntWritable result = new IntWritable();
    private long startMillis;
    private long endMillis;

    public void reduce(MetricsWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //todo combine metrics to one line
        startMillis = System.currentTimeMillis();
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
        endMillis = System.currentTimeMillis();
        context.getCounter(MetricsCounter.DURATION).increment(endMillis - startMillis);
        context.getCounter("Metric Output Reducing Time", String.valueOf(this.hashCode())).increment(endMillis - startMillis);
    }
}
