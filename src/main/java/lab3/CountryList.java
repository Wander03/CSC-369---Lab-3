package lab3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.hash.Hash;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class CountryList {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    // Mapper for Apache HTTP access log
    public static class AddressMapper extends Mapper<LongWritable, Text, Text, Text> {
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
	    context.write(new Text(parts[6]), new Text(hostnameMap.get(address)));
        }
    }

    public static class AddressReducer extends  Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> country, Context context)  throws IOException, InterruptedException {
            HashSet<String> result = new HashSet<>();
            for (Text c : country) {
                result.add(c.toString());
            }
            List<String> sortedResult = new ArrayList<>(result);
            Collections.sort(sortedResult);
            String countryList = String.join(", ", sortedResult);
            context.write(key, new Text(countryList));
        }
	}


}
