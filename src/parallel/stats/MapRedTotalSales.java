package parallel.stats;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MapRedTotalSales {
	
	/*
	 * Emit <1,partialSum> for each line in the split
	 */
	public static class Map extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
		private IntWritable outKey = new IntWritable(1);
		private DoubleWritable partialSum = new DoubleWritable(0.0);
		
		public void map(LongWritable key, Text value, Context context)
					throws IOException, InterruptedException {
			String[] token = value.toString().split(",");
			int numEntries = token.length-1;					// token[0] is the custId
			double sum = 0.0;
			for (int i=0; i<numEntries; i++) {
				sum += Double.parseDouble(token[i+1]);
			}
			partialSum.set(sum);
			context.write(outKey, partialSum);
		}
	}

	/*
	 * Emit <1,partialSum>, where partialSum is the sum of the partialSum's in the mappers in this node
	 */
	public static class Combiner extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
					throws IOException, InterruptedException {
			double sum = 0.0;
			int i=1;
			for (DoubleWritable dw : values) {
				sum += dw.get();
				System.out.printf("i=%d; dw=%.2f; sum=%.2f\n",i,dw.get(),sum);
				i++;
			}
			context.write(key, new DoubleWritable(sum));
		}
	}
	
	/*
	 * Emit <totalSum> to HFDS. Note that there is only one reducer.
	 */
	public static class Reduce extends Reducer<IntWritable, DoubleWritable, NullWritable, Text> {
		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) 
					throws IOException, InterruptedException {
			double sum = 0.0;
			for (DoubleWritable dw : values) {
				sum += dw.get();
			}
			Text total = new Text("Total Sales = " + String.format("%.2f",sum));
			context.write(NullWritable.get(), total);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "totalsales");
		job.setJarByClass(MapRedTotalSales.class);		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(Reduce.class);	
		job.setNumReduceTasks(1);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
