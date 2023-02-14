package lab3;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AdvancedSorting {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, new Text(""));
        }
    }

    public static class AdvancedSortingComparator extends WritableComparator {
        protected AdvancedSortingComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            Text key1 = (Text) wc1;
            String[] parts1 = key1.toString().split("\t");
            Text country1 = new Text(parts1[0]);
            IntWritable count1 = new IntWritable(Integer.parseInt(parts1[2]));

            Text key2 = (Text) wc2;
            String[] parts2 = key2.toString().split("\t");
            Text country2 = new Text(parts2[0]);
            IntWritable count2 = new IntWritable(Integer.parseInt(parts2[2]));

            if (country1.compareTo(country2) == 0){
                return (count2.compareTo(count1));
            }
            return country1.compareTo(country2);
        }
    }

    public static class ReducerImpl extends Reducer<Text, NullWritable, Text, Text> {

        protected void reduce(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }
}
