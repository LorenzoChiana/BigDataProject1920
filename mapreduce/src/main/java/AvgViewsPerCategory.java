import java.io.IOException;
import java.util.*;

import com.cloudera.org.codehaus.jackson.JsonFactory;
import com.cloudera.org.codehaus.jackson.JsonParser;
import com.cloudera.org.codehaus.jackson.JsonToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * MapReduce job to join yellowTaxi.seq and zone.seq.
 */
public class AvgViewsPerCategory {

	/**
	 * First Mapper
	 *
	 * Takes category id and views from file
	 */
	public static class CategoryViewsOccurrenceMapper extends Mapper<Object, Text, Text, IntWritable>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 15);

			String categoryId = tokens[5].replaceAll("^[\"']+|[\"']+$", "");
			String view_count = tokens[8].replaceAll("^[\"']+|[\"']+$", "");
			if (!tokens[5].equals("categoryId") && !tokens[8].equals("view_count")) {
				context.write(new Text(categoryId), new IntWritable(Integer.parseInt(view_count)));
			}
		}
	}

	/**
	 * First Reducer
	 *
	 * Calculates views average
	 */
	public static class CategoryViewsAvgReducer extends Reducer<Text, IntWritable, IntWritable, IntWritable> {
		private final IntWritable res = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0;
			int count = 0;
			for(IntWritable val: values) {
				sum += val.get();
				count++;
			}
			res.set((int)(sum/count));
			context.write(new IntWritable(Integer.parseInt(key.toString())), res);
		}

	}

	/**
	 * Second Mapper 1
	 *
	 * Simple key value mapper
	 */
	public static class BasicKVMapper extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	/**
	 * Second Mapper 2
	 *
	 * Extracts from json file name and id category
	 */
	public static class CategoryNameMapper extends Mapper<Object, Text, Text, Text> {
		static JsonFactory factory = new JsonFactory();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Text id = null;
			Text name = null;
			JsonParser jsonParser = factory.createJsonParser(value.toString());

			while(!jsonParser.isClosed()){
				JsonToken jsonToken = jsonParser.nextToken();
				if(JsonToken.FIELD_NAME.equals(jsonToken)){
					String fieldName = jsonParser.getCurrentName();
					jsonParser.nextToken();
					if(fieldName.equals("id")){
						id = new Text(jsonParser.getText());
					} else if (fieldName.equals("category")){
						/*
							#join#name: #join# is used in the reducer to distinguish
							the two types of value it receives from the 2 type of mappers
							(whether the category name or the average of the views)
						 */
						name = new Text("#join#" + jsonParser.getText());
					}
				}
			}

			// id -> #join#name
			if(id != null && name != null) context.write(id, name);
		}
	}

	/**
	 * Second Reducer
	 *
	 * Joins the 2 type of mapper outputs
	 */
	public static class CategoryNameReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Text category = null;
			Text views = null;

			/*
				Use #join# to distinguish if the value is the category name or the views average
			 */
			for(Text value: values) {
				if (value.toString().contains("#join#")) {
					category = new Text(value.toString().split("#join#",2)[1]);
				} else if(!value.toString().equals("")) {
					views = new Text(value.toString());
				}
			}

			if(views != null) context.write(category, views);
		}
	}

	/**
	 * Third Mapper
	 *
	 * Simple mapper that inverts value with the key
	 */
	public static class InvertKVMapper extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(value, key);
		}
	}

	/**
	 * Sort integer by descending
	 */
	public static class DescendingKeyComparator extends WritableComparator {
		protected DescendingKeyComparator() {
			super(Text.class, true);
		}
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			Text k1 = (Text) w1;
			Text k2 = (Text) w2;
			Integer key1 = Integer.parseInt(k1.toString());
			Integer key2 = Integer.parseInt(k2.toString());
			return -1 * key1.compareTo(key2);
		}
	}

	/**
	 * Third Reducer
	 *
	 * Simple reducer that inverts value with the key
	 */
	public static class InvertKVReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(value, key);
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		ArrayList<Job> jobs = new ArrayList<>();
		Configuration configuration = new Configuration();

		jobs.add( Job.getInstance(configuration, "First job") );
		jobs.add( Job.getInstance(configuration, "Second job") );
		jobs.add( Job.getInstance(configuration, "Third job") );

		for (Job job: jobs){
			job.setJarByClass(AvgViewsPerCategory.class);
		}

		for(int i = 0; i < 7; i++) {
			MultipleInputs.addInputPath(
					jobs.get(0),
					new Path(args[i]),
					TextInputFormat.class,
					CategoryViewsOccurrenceMapper.class
			);
		}

		jobs.get(0).setMapOutputKeyClass(Text.class);
		jobs.get(0).setMapOutputValueClass(IntWritable.class);
		jobs.get(0).setOutputFormatClass(TextOutputFormat.class);
		jobs.get(0).setReducerClass(AvgViewsPerCategory.CategoryViewsAvgReducer.class);

		FileSystem fs = FileSystem.get(new Configuration());
		Path firstJobOutputPath = new Path(args[8]);
		if (fs.exists(firstJobOutputPath)) {
			fs.delete(firstJobOutputPath, true);
		}

		FileOutputFormat.setOutputPath(jobs.get(0), firstJobOutputPath);

		MultipleInputs.addInputPath(jobs.get(1), firstJobOutputPath, KeyValueTextInputFormat.class, BasicKVMapper.class);
		MultipleInputs.addInputPath(jobs.get(1), new Path(args[7]), TextInputFormat.class, CategoryNameMapper.class);

		jobs.get(1).setReducerClass(CategoryNameReducer.class);

		jobs.get(1).setOutputKeyClass(Text.class);
		jobs.get(1).setOutputValueClass(Text.class);


		Path secondJobOutputPath = new Path(args[9]);
		if (fs.exists(secondJobOutputPath)) {
			fs.delete(secondJobOutputPath, true);
		}

		FileOutputFormat.setOutputPath(jobs.get(1), secondJobOutputPath);

		FileInputFormat.addInputPath(jobs.get(2), secondJobOutputPath);

		jobs.get(2).setInputFormatClass(KeyValueTextInputFormat.class);
		jobs.get(2).setMapperClass(InvertKVMapper.class);
		jobs.get(2).setReducerClass(InvertKVReducer.class);
		jobs.get(2).setSortComparatorClass(DescendingKeyComparator.class);

		jobs.get(2).setMapOutputKeyClass(Text.class);
		jobs.get(2).setMapOutputValueClass(Text.class);

		Path thirdJobOutputPath = new Path(args[10]);
		if (fs.exists(thirdJobOutputPath)) {
			fs.delete(thirdJobOutputPath, true);
		}
		FileOutputFormat.setOutputPath(jobs.get(2), thirdJobOutputPath);

		for (Job job: jobs) {
			if (!job.waitForCompletion(true)) {
				System.exit(2);
			}
		}
	}
}