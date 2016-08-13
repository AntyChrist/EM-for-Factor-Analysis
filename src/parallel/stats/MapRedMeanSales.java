package parallel.stats;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MapRedMeanSales {
	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
		private IntWritable custId = new IntWritable();
		private Text mean = new Text();
		
		public void map(LongWritable key, Text value, Context context)
					throws IOException, InterruptedException {
			String[] token = value.toString().split(",");
			int numEntries = token.length-1;					// token[0] is the custId
			double sum = 0.0;
			for (int i=0; i<numEntries; i++) {
				sum += Double.parseDouble(token[i+1]);
			}
			custId.set(Integer.parseInt(token[0]));
			mean.set(String.format("%.2f", sum/numEntries));
			context.write(custId, mean);
		}
	}
	
	public static class Reduce extends Reducer<IntWritable, Text, Text, Text> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context) 
					throws IOException, InterruptedException {
			Text outKey = new Text(String.valueOf(key));
			for (Text val : values) {
				context.write(outKey, val);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "meansales");
		job.setJarByClass(MapRedMeanSales.class);		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(Map.class);		
		job.setReducerClass(Reduce.class);		
		job.setNumReduceTasks(1);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
