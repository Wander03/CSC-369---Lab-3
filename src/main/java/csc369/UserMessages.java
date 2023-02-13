package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class UserMessages {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    // Mapper for hostnames file
    public static class MessageMapper extends Mapper<Text, Text, Text, Text> {

	@Override
        public void map(Text key, Text value, Context context)  throws IOException, InterruptedException {
	    String country = value.toString();
		String out = "A\t"+ country;
		context.write(key, new Text(out));
	    }
    }

    // Mapper for Apache HTTP access log
    public static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String[] parts = value.toString().split(" ");
            String address = parts[0];
	        String out = "B\t1";
	    context.write(new Text(address), new Text(out));
        }
    }

    //  Reducer: just one reducer class to perform the "join"
    public static class JoinReducer extends  Reducer<Text, Text, Text, Text> {

	@Override
	    public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
        int sum = 0;
        Iterator<IntWritable> itr = intOne.iterator();

        while (itr.hasNext()) {
            sum  += itr.next().get();
        }
        result.set(sum);
        for (Text val : values) {
            context.write(key, val);
        }
	    }
	}


}
