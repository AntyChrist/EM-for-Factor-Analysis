/*
 * Author: LIU Sining, 12132473D, The Hong Kong Polytechnic University
 * Version: 2.0
 * Date: July 2016
 * 
 * This file is subject to the terms and conditions defined in
 * file 'license.txt', which is part of this source code package.
*/


package parallel.fa;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.linear.NonPositiveDefiniteMatrixException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import Jama.Matrix;

public class FactorAnalysis {

	int R;		//input vector dimension	
	int D;		//latent factor dimension	
	
	//other variable for EM algorithm
	Matrix V;
	Matrix Sigma;
	Matrix L;

	public FactorAnalysis(int dim, int d){
		this.R = dim;
		this.D = d;
		init();
	}
	//create FA object from FA_FILE
	public FactorAnalysis(int dim, int d, String FA_FILE){
		this.R = dim;
		this.D = d;
		try {
			loadParameters(FA_FILE);
		} catch (IOException e) {
			//FA file not found
			init();
		}
	}
	private void init() {
		V = Matrix.random(R, D);
		Sigma = Matrix.identity(R, R);
		L = Matrix.identity(D, D).plus(V.transpose().times(Sigma.inverse()).times(V));
	}
	public void saveParameters(String faFile) {
		// TODO Auto-generated method stub
		Path pt = new Path(faFile);
		FileSystem fs;
		try {
			fs = FileSystem.get(new Configuration());
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
			br.write(this.toString());
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	private void loadParameters(String fa_FILE) throws IOException {
		Path pt = new Path(fa_FILE);
		FileSystem fs;
		fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(pt)));
		String line; String[] token;
		double [][] v = new double [R][D];
		double [][] sigma = new double [R][R];
		double [][] l = new double [D][D];
		for (int i = 0; i < R; i++) {
			line = br.readLine();
			token = line.split(" ");
			for (int j = 0; j < D; j++) {
				v[i][j] = Double.parseDouble(token[j]);
			}
		}
		for (int i = 0; i < R; i++) {
			line = br.readLine();
			token = line.split(" ");		
			for (int j = 0; j < R; j++) {
				sigma[i][j] = Double.parseDouble(token[j]);
			}
		}
		for (int i = 0; i < D; i++) {
			line = br.readLine();
			token = line.split(" ");		
			for (int j = 0; j < D; j++) {
				l[i][j] = Double.parseDouble(token[j]);
			}
		}
		br.close();
		V = new Matrix(v);
		Sigma = new Matrix(sigma);
		L = new Matrix(l);
	}
	public String toString() {
		StringBuilder sb = new StringBuilder();
		double [][] v = V.getArray();
		double [][] sigma = Sigma.getArray();
		double [][] l = L.getArray();
		for (int i = 0; i < R; i++) {
			for (int j = 0; j < D; j++) {
				sb.append(String.format("%.5f ", v[i][j]));
			}
			sb.append("\n");
		}
		for (int i=0; i < R; i++) {
			for (int j = 0; j < R; j++) {
				sb.append(String.format("%.5f ", sigma[i][j]));
			}
			sb.append("\n");
		}
		for (int i=0; i < D; i++) {
			for (int j = 0; j < D; j++) {
				sb.append(String.format("%.5f ", l[i][j]));
			}
			sb.append("\n");
		}
		return(sb.toString());
	}
	public double logLikelihood(double [] x){
		double llh = 0.0;
		double [] means;
		double [][] covariances;
		means = new double [R];
		//for debugging
		covariances = (V.times(V.transpose())).plus(Sigma).getArray();
		try{
			MultivariateNormalDistribution mnd = new MultivariateNormalDistribution(means, covariances);
			return Math.log((mnd.density(x) + 1) / 2); //apply Laplace smoothing
		}catch(NonPositiveDefiniteMatrixException e){
				//reason for this exception:
				//the value of likelihood function is large
				//the estimation of samples is pretty well
				return 0.0;
			}
		
	}
	public void maximize(Stats stats) {
		this.V = stats.S1.times(stats.S2.inverse());
		this.Sigma = (stats.S3.times(1.0 / (double)stats.noVectors)).minus(this.V.times(stats.S4).times(1.0 / (double)stats.noVectors));
		//for debugging, remove this line
		this.L = Matrix.identity(D, D).plus(V.transpose().times(Sigma.inverse()).times(V));
	}
	
}
