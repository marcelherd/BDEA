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
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Wie viele Wörter kommen insgesamt vor?
// Wie viele davon sind unique?
public class WordSum {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private static final IntWritable ONE = new IntWritable(1);

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString(), " .(-)#\"',;:?!");
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken().toLowerCase());
				context.write(word, ONE);
			}
		}

	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
				context.getCounter("stats", "words").increment(1);
			}
			result.set(sum);
			context.write(key, result);
		}

	}

	public static void main(String[] args) throws Exception {
		args = new String[] { "/home/marcel/Documents/faust", "/tmp/output-wcs/" };

		Util.cleanUp("/tmp/output-wcs/");

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word sum");

		job.setJarByClass(WordSum.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(10);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		int result = job.waitForCompletion(true) ? 0 : 1;
		System.out.println("Total words: " + job.getCounters().findCounter("stats", "words").getValue());
		System.out.println("Unique words: " + job.getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue());
		//Util.printFileOutput("/tmp/output-wcs/part-r-00000");
		System.exit(result);
	}
}
