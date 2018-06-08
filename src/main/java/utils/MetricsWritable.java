package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class MetricsWritable implements WritableComparable<MetricsWritable>, Cloneable {

    private Metric metric;
    private Text project;
    private Text file;

    public MetricsWritable() {
        project = new Text();
        file = new Text();
    }

    public MetricsWritable(Text project, Text file) {
        this.project = project;
        this.file = file;
    }

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeEnum(dataOutput, metric);
        project.write(dataOutput);
        file.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        metric = WritableUtils.readEnum(dataInput, Metric.class);
        project.readFields(dataInput);
        file.readFields(dataInput);
    }

    @Override
    public int compareTo(MetricsWritable other) {
        int cmp = metric.compareTo(other.metric);
        if (cmp == 0) {
            cmp = project.compareTo(other.project);
            if (cmp == 0) {
                cmp = file.compareTo(other.file);
            }
        }
        return cmp;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MetricsWritable)) {
            return false;
        } else {
            MetricsWritable other = (MetricsWritable) o;
            return this.metric.equals(other.getMetric()) && this.project.equals(other.getProject()) && this.file.equals(other.getFile());
        }
    }

    public void setMetric(Metric metric) {
        this.metric = metric;
    }

    public Metric getMetric() {
        return metric;
    }

    public void setFile(String file) {
        this.file = new Text(file);
    }

    public void setProject(String project) {
        this.project = new Text(project);
    }

    public Text getFile() {
        return file;
    }


    public Text getProject() {
        return project;
    }

    @Override
    public String toString() {
        return metric + ";" + project + ";" + file;
    }
}
