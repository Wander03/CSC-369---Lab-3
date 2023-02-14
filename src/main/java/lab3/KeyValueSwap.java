package lab3;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class KeyValueSwap {

    public static final Class OUTPUT_KEY_CLASS = IntWritable.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, IntWritable, Text> {
        private Text newValue = new Text();
        private IntWritable newKey = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            newValue.set(parts[0]);
            newKey.set(Integer.parseInt(parts[1]));
            context.write(newKey, newValue);
        }
    }

    public static class DescendingKeyComparator extends WritableComparator {
        protected DescendingKeyComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            IntWritable key1 = (IntWritable) wc1;
            IntWritable key2 = (IntWritable) wc2;
            return key2.compareTo(key1);
        }
    }

    public static class ReducerImpl extends Reducer<IntWritable, Text, IntWritable, Text> {

        protected void reduce(IntWritable count, Text path, Context context) throws IOException, InterruptedException {
            context.write(count, path);
        }
    }
}
