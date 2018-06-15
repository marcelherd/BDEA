package com.marcelherd.bdea.dokfreq;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.StringTokenizer;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DokumentenFrequenz {
	
	// DF(ti) = in wie vielen Dokumenten kommt ti vor
	
	public static class DokFreqMapper extends Mapper<Object, Text, Text, IntWritable> {

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
	
	public static class DokFreqCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

		private static final IntWritable ONE = new IntWritable(1);

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, ONE);
		}

	}
	
	public static class DokFreqReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
		}

	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Dokumentenfrequenz");
		
		job.setJarByClass(DokumentenFrequenz.class);
		job.setMapperClass(DokFreqMapper.class);
		job.setCombinerClass(DokFreqCombiner.class);
		job.setReducerClass(DokFreqReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		Path inputPath = new Path("/home/marcel/Documents/uploaded_files/");
		Path outputPath = new Path("/home/marcel/Documents/processed_files/");
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		int result = job.waitForCompletion(true) ? 0 : 1;
		if (result == 0) {
			try (MyCassandra cassandra = new MyCassandra()) {
				System.out.println("Writing to database...");
				try (Stream<String> stream = Files.lines(Paths.get("/home/marcel/Documents/processed_files/part-r-00000"))) {
					stream
						.map(s -> s.trim())
						.map(s -> s.split("\\s+", 2)).forEach(cassandra::insert);
				}
			}
		}
		// batch write
		System.exit(result);
	}

}
