package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class VertexWritable implements Writable, Cloneable {

    private Text message;
    private List<Text> edges;
    private IntWritable value;
    private BooleanWritable isNew = new BooleanWritable(true);

    public VertexWritable() {
        super();
        this.value = new IntWritable();
    }

    public VertexWritable(Text message) {
        super();
        this.message = message;
        this.value = new IntWritable();
    }

    public void write(DataOutput dataOutput) throws IOException {
        if(message == null) {
            dataOutput.writeBoolean(false);
        } else {
            dataOutput.writeBoolean(true);
            message.write(dataOutput);
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
        isNew.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        if(dataInput.readBoolean()) {
            message = new Text();
            message.readFields(dataInput);
        } else {
            message = null;
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
        isNew.readFields(dataInput);
    }

    @Override
    public String toString() {
        return "VertexWritable [vertexId=" + message + ", pointsTo=" + edges + ", value=" + value + ", isNew=" + isNew + "]";
    }

    @Override
    public VertexWritable clone() {
        VertexWritable toReturn;
        if(message != null) {
            toReturn = new VertexWritable(new Text(message));
        } else {
            toReturn = new VertexWritable();
        }
        if (edges != null) {
            toReturn.edges = new ArrayList<>();
            for (Text l : edges) {
                toReturn.edges.add(new Text(l.toString()));
            }
        }
        toReturn.setValue(new IntWritable(value.get()));
        toReturn.setIsNew(new BooleanWritable(true));
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

    public Text getMessage() {
        return message;
    }

    public IntWritable getValue() {
        return value;
    }

    public void setValue(IntWritable value) {
        this.value = value;
    }

    public BooleanWritable getIsNew() {
        return isNew;
    }

    public void setIsNew(BooleanWritable isNew) {
        this.isNew = isNew;
    }
}
