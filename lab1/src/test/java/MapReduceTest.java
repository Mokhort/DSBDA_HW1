import eu.bitwalker.useragentutils.UserAgent;
import bdtc.lab1.HW1Mapper;
import bdtc.lab1.HW1Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class MapReduceTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, Text> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, Text> mapReduceDriver;

    String testMapper = "1, 1621922090, 245";
    String testMapper1 = "1, 1621922090, 300";
    String testReducer = "1621922090, 245";

    private UserAgent userAgent;
    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testMapper))
                //.withOutput(new Text("1621922090, 245"), new IntWritable(2))
                .withOutput(new Text(testReducer), new IntWritable(1))
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        reduceDriver
                .withInput(new Text(testReducer), values)
                .withOutput(new Text("1621922040, 1m, 245"), new Text("Node1CPU"))
                .runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testMapper))
                .withOutput(new Text("1621922040, 1m, 245"), new Text("Node1CPU"))
                .runTest();
    }
}
