package com.marcelherd.bdea.uebung1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NumberSum {
	
	public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
		
		private static final IntWritable ONE = new IntWritable(1);
		
		private IntWritable number = new IntWritable();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			while (tokenizer.hasMoreTokens()) {
				number.set(Integer.parseInt(tokenizer.nextToken()));
				context.write(ONE, number);
			}
		}
		
	}
	
	public static class IntSumReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		
		private IntWritable result = new IntWritable();
		
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			
			for (IntWritable value : values) {
				sum += value.get();
			}
			
			result.set(sum);
			context.write(key, result);
		}
		
	}

	public static void main(String[] args) throws Exception {
		args = new String[] { "/home/marcel/Documents/numbers", "/tmp/output-sum/" };
		
		Util.cleanUp("/tmp/output-sum/");
		
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "sum numbers");
		
		job.setJarByClass(NumberSum.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		// job.setNumReduceTasks(3);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		int result = job.waitForCompletion(true) ? 0 : 1;
		Util.printFileOutput("/tmp/output-sum/part-r-00000");
		System.exit(result);
	}

}
