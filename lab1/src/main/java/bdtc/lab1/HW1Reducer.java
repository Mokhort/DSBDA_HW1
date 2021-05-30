package bdtc.lab1;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

@Log4j
public class HW1Reducer extends Reducer<Text, IntWritable, Text, Text> {
    Text keyWritable = new Text();
    private static String[] values_key = {"Node1CPU","Node2RAMmb"};
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        double sum_val = 0;
        int counter = 0;
        int scale = 1;
        double timestamp = 0;
        String[] array =new String[2];
        Text result = new Text();
        for (IntWritable val : values) {
            int element = val.get()-1;
            keyWritable.set(new Text(values_key[element]));
            System.out.println("key " + keyWritable );
            System.out.println("element " + element);
            String line = key.toString();
            array = line.split(", ");
            if (!array[0].isEmpty() && !array[array.length - 1].isEmpty()) {
                log.info(array[0]);
                log.info(array[1]);
            }
            sum_val+=Double.valueOf(array[1]);
            counter+=1;

        }
        timestamp= Math.floor(Double.valueOf(array[0])/60)*60;
        sum_val = sum_val/counter;
        System.out.println("sum_val " + sum_val);
        result = new Text((int)timestamp+", "+scale+"m, " + (int)sum_val);
        context.write(result,keyWritable);
        System.out.println("dict " + keyWritable + " val" + sum_val);
    }
}
