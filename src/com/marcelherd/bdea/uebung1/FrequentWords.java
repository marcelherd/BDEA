package com.marcelherd.bdea.uebung1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class FrequentWords {
	
	// JOB 1 MAPPER
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private static final IntWritable ONE = new IntWritable(1);

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString(), " .(-)#\"',;:?!");
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, ONE);
			}
		}

	}
	
	// JOB 2 MAPPER
	public static class OutputTokenizerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
			Text word = new Text(stringTokenizer.nextToken());
			IntWritable count = new IntWritable(Integer.parseInt(stringTokenizer.nextToken()));
			context.write(count, word);
		}
		
	}
	
	// JOB 3 Mapper
	public static class StandardMapper extends Mapper<IntWritable, Text, IntWritable, Text> {
		public void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	// JOB 1 REDUCER
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}

	}
	
	// JOB 2 Reducer
	public static class StandardReducer  extends Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(key,  val);
			}
		}
	}
	
	// JOB 3 REDUCER
	public static class OutputReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				// Häufigste Worte
				//System.out.println(key.get() + " " + val.toString());
				
				// Häufigste Redner
				if (val.toString().length() > 1 && Character.isUpperCase(val.toString().charAt(0)) && Character.isUpperCase(val.toString().charAt(1))) {
					System.out.println(key.get() + " " + val.toString());
				}
				
				context.write(key, val);
			}
		}
		
	}
	
	public static class DescendingSortComparator extends WritableComparator {
		
		public DescendingSortComparator() {
			super(IntWritable.class, true);
		}
		
		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a, WritableComparable b) {
			return super.compare(a, b) * -1;
		}
		
	}

	public static void main(String[] args) throws Exception {
		args = new String[] { "/home/marcel/Documents/faust", "/tmp/output-fwwc/", "/tmp/output-fwsort", "/tmp/output-fw" };

		Util.cleanUp("/tmp/output-fwwc/");
		Util.cleanUp("/tmp/output-fwsort/");
		Util.cleanUp("/tmp/output-fw/");

		Configuration counterConf = new Configuration();
		Job counterJob = Job.getInstance(counterConf, "word count");
		
		FileInputFormat.addInputPath(counterJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(counterJob, new Path(args[1]));

		counterJob.setJarByClass(FrequentWords.class);
		counterJob.setMapperClass(TokenizerMapper.class);
		counterJob.setCombinerClass(IntSumReducer.class);
		counterJob.setReducerClass(IntSumReducer.class);
		
		counterJob.setNumReduceTasks(3);
		
		counterJob.setOutputKeyClass(Text.class);
		counterJob.setOutputValueClass(IntWritable.class);

		counterJob.waitForCompletion(true);
		
		// job 2
		Configuration sorterConf = new Configuration();
		Job sorterJob = Job.getInstance(sorterConf, "sort");
		
		FileInputFormat.addInputPath(sorterJob, new Path(args[1]));
		FileOutputFormat.setOutputPath(sorterJob, new Path(args[2]));
		
		sorterJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		sorterJob.setJarByClass(FrequentWords.class);
		sorterJob.setMapperClass(OutputTokenizerMapper.class);
		sorterJob.setReducerClass(StandardReducer.class);
		sorterJob.setSortComparatorClass(DescendingSortComparator.class);
		
		sorterJob.setNumReduceTasks(3);
		
		sorterJob.setOutputKeyClass(IntWritable.class);
		sorterJob.setOutputValueClass(Text.class);
		
		sorterJob.waitForCompletion(true);
		
		// job 3
		Configuration outputConf = new Configuration();
		Job outputJob = Job.getInstance(sorterConf, "partitioner");
		
		FileInputFormat.addInputPath(outputJob, new Path(args[2]));
		FileOutputFormat.setOutputPath(outputJob, new Path(args[3]));
		
		outputJob.setInputFormatClass(SequenceFileInputFormat.class);
		
		outputJob.setJarByClass(FrequentWords.class);
		outputJob.setMapperClass(StandardMapper.class);
		outputJob.setReducerClass(OutputReducer.class);
		outputJob.setSortComparatorClass(DescendingSortComparator.class);
		
		outputJob.setNumReduceTasks(3);
		
		TotalOrderPartitioner.setPartitionFile(outputConf, new Path("/tmp/pout"));
		InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<IntWritable, Text>(0.05, 1000, 4);
		InputSampler.writePartitionFile(outputJob, sampler);
		outputJob.setPartitionerClass(TotalOrderPartitioner.class);	
		
		outputJob.setOutputKeyClass(IntWritable.class);
		outputJob.setOutputValueClass(Text.class);
		
		int result = outputJob.waitForCompletion(true) ? 0 : 1;
		System.exit(result);
	}

}
