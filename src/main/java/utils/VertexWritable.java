package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class VertexWritable implements Writable, Cloneable {

    private Text vertex;
    private List<Text> edges;
    private IntWritable value;

    public VertexWritable() {
        super();
    }

    public VertexWritable(Text VertexId) {
        super();
        this.vertex = VertexId;
        this.value = new IntWritable();
    }

    public void write(DataOutput dataOutput) throws IOException {
        if(vertex == null) {
            dataOutput.writeBoolean(false);
        } else {
            dataOutput.writeBoolean(true);
            vertex.write(dataOutput);
        }
        if (edges == null) {
            dataOutput.writeInt(-1);
        } else {
            dataOutput.writeInt(edges.size());
            for (Text l : edges) {
                l.write(dataOutput);
            }
        }
        value.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        if(dataInput.readBoolean()) {
            vertex = new Text();
            vertex.readFields(dataInput);
        } else {
            vertex = null;
        }
        int length = dataInput.readInt();
        if (length > -1) {
            edges = new ArrayList<>();
            for (int i = 0; i < length; i++) {
                Text temp = new Text();
                temp.readFields(dataInput);
                edges.add(temp);
            }
        } else {
            edges = null;
        }
        value = new IntWritable();
        value.readFields(dataInput);
    }

    @Override
    public String toString() {
        return "VertexWritable [vertexId=" + vertex + ", pointsTo=" + edges + ", value=" + value + "]";
    }

    @Override
    protected VertexWritable clone() {
        VertexWritable toReturn = new VertexWritable(new Text(vertex));
        if (edges != null) {
            toReturn.edges = new ArrayList<>();
            for (Text l : edges) {
                toReturn.edges.add(new Text(l.toString()));
            }
        }
        toReturn.setValue(new IntWritable(value.get()));
        return toReturn;
    }

    public boolean isMessage() {
        if (edges == null)
            return true;
        else
            return false;
    }

    public List<Text> getEdges() {
        return edges;
    }

    public void addVertex(Text className) {
        if (edges == null)
            edges = new ArrayList<>();
        edges.add(className);
    }

    public Text getVertex() {
        return vertex;
    }

    public IntWritable getValue() {
        return value;
    }

    public void setValue(IntWritable value) {
        this.value = value;
    }
}