package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class VertexWritable implements Writable, Cloneable {

    private IntWritable vertex;
    private TreeSet<Text> edges;

    public VertexWritable() {
        super();
    }

    public VertexWritable(IntWritable VertexId) {
        super();
        this.vertex = VertexId;
    }

    public void write(DataOutput dataOutput) throws IOException {
        vertex.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        vertex = new IntWritable();
        vertex.readFields(dataInput);
    }

    @Override
    public String toString() {
        return "VertexWritable [minimalVertexId=" + vertex + ", pointsTo=" + edges + "]";
    }

    @Override
    protected VertexWritable clone() {
        VertexWritable toReturn = new VertexWritable(new IntWritable(vertex.get()));
        if (edges != null) {
            toReturn.edges = new TreeSet<>();
            for (Text l : edges) {
                toReturn.edges.add(new Text(l.toString()));
            }
        }
        return toReturn;
    }

    public TreeSet<Text> getEdges() {
        return edges;
    }

    public void setEdges(TreeSet<Text> edges) {
        this.edges = edges;
    }

    public IntWritable getVertex() {
        return vertex;
    }

    public void setVertex(IntWritable vertex) {
        this.vertex = vertex;
    }
}
