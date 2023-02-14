package csc369;

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
	} else if ("UserMessages".equalsIgnoreCase(otherArgs[0])) {

//	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
//				TextInputFormat.class, CountryCount.UserMapper.class );
//	    MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
//				KeyValueTextInputFormat.class, CountryCount.MessageMapper.class );
//
//	    job.setReducerClass(CountryCount.JoinReducer.class);
//
//	    job.setOutputKeyClass(CountryCount.OUTPUT_KEY_CLASS);
//	    job.setOutputValueClass(CountryCount.OUTPUT_VALUE_CLASS);
//	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

	} else if ("KeyValueSwap".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(KeyValueSwap.ReducerImpl.class);
		job.setMapperClass(KeyValueSwap.MapperImpl.class);
		job.setSortComparatorClass(KeyValueSwap.DescendingKeyComparator.class);
		job.setOutputKeyClass(KeyValueSwap.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(KeyValueSwap.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("CountryCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(CountryCount.AddressReducer.class);
	    job.setMapperClass(CountryCount.AddressMapper.class);
	    job.setOutputKeyClass(CountryCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(CountryCount.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	}  else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}
        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
