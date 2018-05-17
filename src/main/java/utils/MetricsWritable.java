package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class MetricsWritable implements WritableComparable<MetricsWritable>, Cloneable {

    private Text metric;
    private Text project;
    private Text file;

    public MetricsWritable() {
        metric = new Text();
        project = new Text();
        file = new Text();
    }

    public MetricsWritable(Text project, Text file) {
        metric = new Text();
        this.project = project;
        this.file = file;
    }

    public void write(DataOutput dataOutput) throws IOException {
        metric.write(dataOutput);
        project.write(dataOutput);
        file.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        metric.readFields(dataInput);
        project.readFields(dataInput);
        file.readFields(dataInput);
    }

    public int compareTo(MetricsWritable other) {
        int cmp = metric.compareTo(other.metric);
        if (cmp == 0) {
            cmp = project.compareTo(other.project);
            if(cmp == 0) {
                cmp = file.compareTo(other.file);
            }
        }
        return cmp;
    }

    public void setMetric(String metric) {
        this.metric = new Text(metric);
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

    public Text getMetric() {
        return metric;
    }

    public Text getProject() {
        return project;
    }

    @Override
    public String toString(){
        return metric + ";" + project + ";" + file;
    }
}
