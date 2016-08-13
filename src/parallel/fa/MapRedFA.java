/*
 * Author: LIU Sining, 12132473D, The Hong Kong Polytechnic University
 * Version: 2.0
 * Date: July 2016
 * 
 * This file is subject to the terms and conditions defined in
 * file 'license.txt', which is part of this source code package.
*/


package parallel.fa;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import Jama.Matrix;

import parallel.fa.Config;



public class MapRedFA {
	
	private final static int DIM = Config.DIM;
	private final static int NO_LATENT_FACTOR = Config.NO_LATENT_FACTOR;
	private final static String FA_FILE = Config.FA_FILE;
	
	// TODO implement fa file reading
	private static FactorAnalysis fa = new FactorAnalysis(DIM, NO_LATENT_FACTOR, FA_FILE);
	
	public static class FAMapper extends Mapper <LongWritable, Text, LongWritable, Stats>{
		
		private final static LongWritable keyOut = new LongWritable(1);
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] token = value.toString().split("\\s+|,");	 //one or more whitespace and ,
			double[] xt = new double[DIM];						 //contains line vector
			for (int i = 0; i < DIM; i++) {
				xt[i] = Double.parseDouble(token[i]);
			}
			Stats stats = new Stats ();
			//If Sigma in post-mean is singular, re-run the program. 
			//This is because of the improper initialization of V and Sigma.
			//Or curse-of-dimensionality happens
			stats.accumulate(xt, fa);		
			stats.likelh = fa.logLikelihood(xt); 	
			context.write(keyOut, stats);
		}
	}
	public static class FACombiner extends Reducer<LongWritable, Stats, LongWritable, Stats> {

		public void reduce(LongWritable key, Iterable<Stats> values, Context context) 
				throws IOException, InterruptedException {
			Iterator<Stats> iter = values.iterator();
			Stats stats = iter.next();
			while (iter.hasNext()) {
				Stats thisStats = iter.next();
				stats.accumulate(thisStats);
			}
			context.write(key, stats);
		}
	}
	public static class FAReducer extends Reducer <LongWritable, Stats, LongWritable, Text>{
		
		public void reduce(LongWritable key, Iterable<Stats> values, Context context)
				throws IOException, InterruptedException {
			
			Iterator<Stats> iter = values.iterator();
			Stats stats = iter.next();
			while (iter.hasNext()) {
				Stats thisStats = iter.next();
				stats.accumulate(thisStats);
			}
			fa.maximize(stats);
			System.out.println(fa.toString() + "\nLogLikelihood=" + stats.likelh);
			fa.saveParameters(FA_FILE);
			Text valueOut = new Text();
			valueOut.set(fa.toString() + "\nLogLikelihood=" + stats.likelh);
			context.write(key, valueOut);
		}
	}
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"fa");
		job.setJarByClass(MapRedFA.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Stats.class);
		
		job.setMapperClass(FAMapper.class);
		job.setReducerClass(FAReducer.class);
		job.setCombinerClass(FACombiner.class);
		job.setNumReduceTasks(1);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		

	}

}
