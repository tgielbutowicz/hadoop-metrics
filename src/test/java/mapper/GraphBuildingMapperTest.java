package mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import utils.Metric;
import utils.MetricsWritable;
import utils.VertexWritable;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class GraphBuildingMapperTest {

    GraphBuildingMapper mapper;
    Context context;

    @BeforeEach
    void setUp() {
        mapper = new GraphBuildingMapper();
        context = mock(Context.class);
    }

    @Test
    void shouldEmitMessage() throws IOException, InterruptedException {
        MetricsWritable key = new MetricsWritable();
        key.setMetric(Metric.DIT);
        key.setClassName("file");
        VertexWritable value = new VertexWritable(new Text("message"));
        value.addVertex(new Text("subclass"));
        value.addVertex(new Text("superclass"));

        mapper.map(key, value, context);

        verify(context,times(2)).write(any(),any());
    }
}