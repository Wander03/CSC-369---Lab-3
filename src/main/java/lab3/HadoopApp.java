package lab3;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        
        Job job = new Job(conf, "Lab 3");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> [<input dir>]+ <output dir>");
	    System.exit(-1);
	} else if ("KeyValueSwap".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(KeyValueSwap.ReducerImpl.class);
		job.setMapperClass(KeyValueSwap.MapperImpl.class);
		job.setSortComparatorClass(KeyValueSwap.DescendingKeyComparator.class);
		job.setOutputKeyClass(KeyValueSwap.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(KeyValueSwap.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("AdvancedSorting".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(AdvancedSorting.ReducerImpl.class);
		job.setMapperClass(AdvancedSorting.MapperImpl.class);
		job.setSortComparatorClass(AdvancedSorting.AdvancedSortingComparator.class);
		job.setOutputKeyClass(AdvancedSorting.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(AdvancedSorting.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("CountryCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(CountryCount.AddressReducer.class);
	    job.setMapperClass(CountryCount.AddressMapper.class);
	    job.setOutputKeyClass(CountryCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(CountryCount.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	}  else if ("URLCount".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(URLCount.AddressReducer.class);
		job.setMapperClass(URLCount.AddressMapper.class);
		job.setOutputKeyClass(URLCount.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(URLCount.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	}  else if ("CountryList".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(CountryList.AddressReducer.class);
		job.setMapperClass(CountryList.AddressMapper.class);
		job.setOutputKeyClass(CountryList.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(CountryList.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}
        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
