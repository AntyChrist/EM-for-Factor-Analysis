/*
 * Author: LIU Sining, 12132473D, The Hong Kong Polytechnic University
 * Version: 2.0
 * Date: July 2016
 * 
 * This file is subject to the terms and conditions defined in
 * file 'license.txt', which is part of this source code package.
*/

package parallel.fa;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

import Jama.Matrix;

import parallel.fa.Config;

public class Stats implements Writable {
	
	//private final static int NUM_VEC = Config.NUM_VEC;
	private int R = Config.DIM;	
	private int D = Config.NO_LATENT_FACTOR;	
	int noVectors;
	Matrix S1, S2, S3, S4; 	//partial sum of four statistics
	double likelh;
	
	/*
	 * Note: All Writable implementations must have a default constructor so that the MapReduce 
	 * framework can instantiate them, and populate their fields by calling readFields().
	 * https://www.safaribooksonline.com/library/view/hadoop-the-definitive/9781449328917/ch04.html.
	 */	
	public Stats(){
		noVectors = 0;
		S1 = new Matrix(R,D,0);
		S2 = new Matrix(D,D,0);
		S3 = new Matrix(R,R,0);
		S4 = new Matrix(D,R,0);
		likelh = 0.0;
	}
	
	public Matrix toMatrix (double [] vector){
		Matrix result = new Matrix (vector.length, 1, 0);
		for(int i = 0; i < vector.length; i++)
			result.set(i, 0, vector[i]);
		return result;
	}
	public Matrix post_mean(Matrix x, FactorAnalysis fa){
		return (fa.L.inverse()).times(fa.V.transpose()).times(fa.Sigma.inverse()).times(x);
	}
	public Matrix post_moment(Matrix x, FactorAnalysis fa){
		return (fa.L.inverse()).plus(post_mean(x, fa).times(post_mean(x, fa).transpose()));
	}
	public void accumulate(double[] vector, FactorAnalysis fa){
		noVectors++;
		Matrix x = toMatrix (vector);
		S1.plusEquals(x.times((post_mean(x, fa).transpose())));
		S2.plusEquals(post_moment(x, fa));
		S3.plusEquals(x.times(x.transpose()));
		S4.plusEquals(post_mean(x, fa).times(x.transpose()));
	}
	public void accumulate(Stats thatStats) {
		this.noVectors += thatStats.noVectors;
		this.S1.plusEquals(thatStats.S1);
		this.S2.plusEquals(thatStats.S2);
		this.S3.plusEquals(thatStats.S3);
		this.S4.plusEquals(thatStats.S4);
		likelh += thatStats.likelh;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(noVectors);
		for (double[] dArray : S1.getArray()) {
			writeDoubleArray(out, dArray);
		}
		for (double[] dArray : S2.getArray()) {
			writeDoubleArray(out, dArray);
		}
		for (double[] dArray : S3.getArray()) {
			writeDoubleArray(out, dArray);
		}
		for (double[] dArray : S4.getArray()) {
			writeDoubleArray(out, dArray);
		}
		out.writeDouble(likelh);
	}
	
	private void writeDoubleArray(DataOutput out, double[] dArray) throws IOException {
		for (double d : dArray) {
			out.writeDouble(d);
		}
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		noVectors = in.readInt();
		for (double[] dArray : S1.getArray()) {
			readDoubleArray(in, dArray);
		}
		for (double[] dArray : S2.getArray()) {
			readDoubleArray(in, dArray);
		}
		for (double[] dArray : S3.getArray()) {
			readDoubleArray(in, dArray);
		}
		for (double[] dArray : S4.getArray()) {
			readDoubleArray(in, dArray);
		}
		likelh = in.readDouble();
	}		
	
	private void readDoubleArray(DataInput in, double[] dArray) throws IOException {
		for (int i=0; i<dArray.length; i++) {
			dArray[i] = in.readDouble();
		}
	}
	
}
