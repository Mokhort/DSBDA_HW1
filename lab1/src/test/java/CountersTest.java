import eu.bitwalker.useragentutils.UserAgent;
import bdtc.lab1.CounterType;
import bdtc.lab1.HW1Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;


public class CountersTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    private final String testMalformed = "mama mila ramu";
    private final String test = "2, 1621922090, 245";

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapperCounterOne() throws IOException  {
        mapDriver
                .withInput(new LongWritable(), new Text(testMalformed))
                .runTest();
        assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }

    /*1 good*/
    @Test
    public void testMapperCounterZero() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(test))
                .withOutput(new Text("1621922090, 245"), new IntWritable(2))
                .runTest();
        assertEquals("Expected 1 counter increment", 0, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }
    @Test
    public void testMapperCounters() throws IOException {
        UserAgent userAgent = UserAgent.parseUserAgentString(test);
        mapDriver
                .withInput(new LongWritable(), new Text(test))
                .withInput(new LongWritable(), new Text(testMalformed))
                .withInput(new LongWritable(), new Text(testMalformed))
                .withOutput(new Text("1621922090, 245"), new IntWritable(2))
                .runTest();

        assertEquals("Expected 2 counter increment", 2, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }
}

