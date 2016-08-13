/*
 * Author: LIU Sining, 12132473D, The Hong Kong Polytechnic University
 * Version: 2.0
 * Date: July 2016
 * 
 * This file is subject to the terms and conditions defined in
 * file 'license.txt', which is part of this source code package.
*/

package sequential.fa;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

import org.apache.commons.math3.distribution.MultivariateNormalDistribution;

import Jama.Matrix;

public class FADataGenerator {
	//generate a set of data based on FA distribution
	//assume mean value is zero
	public Matrix V;
	public Matrix Sigma;
	public int R, D;
	public FADataGenerator(int R, int D){
		this.R = R; this.D = D;
		//random generate V and Sigma
		V = Matrix.random(R, D);
		Sigma = Matrix.identity(R, R).times(Math.random());
	}
	public Matrix toMatrix (double [] vector){
		Matrix result = new Matrix (vector.length, 1, 0);
		for(int i = 0; i < vector.length; i++)
			result.set(i, 0, vector[i]);
		return result;
	}
	public Matrix generateSamples (int noSamples){
		
		double [][] content = new double [noSamples][R];
		double [] zMeans = new double[D];
		double [][] zCovariances = Matrix.identity(D, D).getArray();
		MultivariateNormalDistribution zDistribution = new MultivariateNormalDistribution(zMeans, zCovariances);
		double [] eMeans = new double[R];
		MultivariateNormalDistribution eDistribution = new MultivariateNormalDistribution(eMeans, Sigma.getArray());
		for(int i = 0; i < noSamples; i++)
			content[i] = (V.times(toMatrix(zDistribution.sample())).plus(toMatrix(eDistribution.sample()))).getColumnPackedCopy();
		Matrix result = new Matrix (content);
		return result.transpose();
	}
	/* Sample usage of this program:
	public static void main(String[] args){
		FADataGenerator dg = new FADataGenerator(10,4);
		Matrix result = dg.generateSamples(1000);
		result.print(10, 3);
	}
	*/
	
}
