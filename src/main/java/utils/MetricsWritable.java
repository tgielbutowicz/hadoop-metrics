package utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MetricsWritable implements WritableComparable<MetricsWritable>, Cloneable {

    private Metric metric;
    private Text className;

    public MetricsWritable() {
        className = new Text();
    }

    public MetricsWritable(Text className) {
        this.className = className;
    }

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeEnum(dataOutput, metric);
        className.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        metric = WritableUtils.readEnum(dataInput, Metric.class);
        className.readFields(dataInput);
    }

    @Override
    public int compareTo(MetricsWritable other) {
        int cmp = metric.compareTo(other.metric);
        if (cmp == 0) {
            cmp = className.compareTo(other.className);
        }
        return cmp;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MetricsWritable)) {
            return false;
        } else {
            MetricsWritable other = (MetricsWritable) o;
            return this.metric.equals(other.getMetric()) && this.className.equals(other.getClassName());
        }
    }

    public void setMetric(Metric metric) {
        this.metric = metric;
    }

    public Metric getMetric() {
        return metric;
    }

    public void setClassName(String className) {
        this.className = new Text(className);
    }

    public Text getClassName() {
        return className;
    }

    @Override
    public String toString() {
        return metric + ";" + className;
    }
}
