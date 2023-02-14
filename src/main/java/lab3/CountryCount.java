package lab3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CountryCount {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    // Mapper for Apache HTTP access log
    public static class AddressMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Map<String, String> hostnameMap;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            hostnameMap = new HashMap<>();
            BufferedReader reader = new BufferedReader(new FileReader("input_host_name/hostname_country.csv"));

            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                hostnameMap.put(parts[0], parts[1]);
            }
            reader.close();
        }

        @Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String[] parts = value.toString().split(" ");
            String address = parts[0];
	    context.write(new Text(hostnameMap.get(address)), new IntWritable(1));
        }
    }

    public static class AddressReducer extends  Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> intOne, Context context)  throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable intWritable : intOne) {
                sum += intWritable.get();
            }
            result.set(sum);
            context.write(key, result);
        }
	}


}
