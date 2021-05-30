package bdtc.lab1;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.regex.Pattern;

@Log4j
// Mapper handle input data and push value to reducer
public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        //match with input value
        if (Pattern.matches("\\d,\\s\\d{10},\\s\\d+", line)) {
            log.info("matches");
            String[] ml = line.split(", ", 2);
            //value - 1/2 key - string with data
            if (!ml[0].isEmpty() && !ml[ml.length - 1].isEmpty()) {
                word.set(ml[1]);
                one.set(Integer.parseInt(ml[0]));
                log.info(one);
                log.info(word);
                context.write(word,one);
            }
        }
        else {
            log.info("not matches");
            context.getCounter(CounterType.MALFORMED).increment(1);
        }
    }
}