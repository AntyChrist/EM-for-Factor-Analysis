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
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;

import Jama.Matrix;

public class FactorAnalysis {

	int R;		//input vector dimension
	Matrix X;	//input vectors matrix	
	int D;		//latent factor dimension	
	int N;		//no. of training vectors
	Matrix m;	//mean vector
	
	//other variable for EM algorithm
	Matrix V;
	Matrix Sigma;
	Matrix L;
	
	//posterior mean and posterior moment for E step
	ArrayList <Matrix> postMeans;
	ArrayList <Matrix> postMoments;
	
	public FactorAnalysis(Matrix X, int d){
		this.X = X;
		R = X.getRowDimension(); 
		D = d;
		N = X.getColumnDimension();
		//create object for mean vector
		m = new Matrix(R, 1, 0);
		//sum each vector and calculate mean vector
		for(int i = 0; i < N; i++)
			m.plusEquals(X.getMatrix(0, R-1, i, i));
		m = m.times(1.0/(double)N);		
		//create object for posterior mean and posterior
		postMeans = new ArrayList <Matrix> (N);
		postMoments = new ArrayList <Matrix> (N);
	}
	public void initE(){
		//initial E-step
		V = Matrix.random(R, D);
		//V = new Matrix (R, D, 1);
		Sigma = Matrix.identity(R, R);
		L = Matrix.identity(D, D).plus((V.transpose()).times(Sigma.inverse()).times(V));
		//compute posterior mean and posterior moment for each vector
		for(int i = 0; i < N; i++){
			postMeans.add(post_mean(X.getMatrix(0, R-1, i, i)));
			postMoments.add(post_moment(X.getMatrix(0, R-1, i, i)));
		}	
	}
	public void eStep(){
		L = Matrix.identity(D, D).plus(V.transpose().times(Sigma.inverse()).times(V));
		//compute posterior mean and posterior moment for each vector
		for(int i = 0; i < N; i++){
			postMeans.add(post_mean(X.getMatrix(0, R-1, i, i)));
			postMoments.add(post_moment(X.getMatrix(0, R-1, i, i)));
		}	
	}
	public void mStep(){
		Matrix lhs, rhs, x;
		lhs = new Matrix(R,D,0);
		rhs = new Matrix(D,D,0);
		//get new V matrix
		for(int i = 0; i < N ; i++){
			x = X.getMatrix(0, R-1, i, i);
			lhs.plusEquals((x.minus(m)).times((postMeans.get(i).transpose())));
			rhs.plusEquals(postMoments.get(i));
		}
		V = lhs.times(rhs.inverse());
		//get new Sigma matrix
		Matrix diff;
		for(int i = 0; i < N ; i++){
			x = X.getMatrix(0, R-1, i, i);
			diff = x.minus(m);
			Sigma.plusEquals(diff.times(diff.transpose()).minus(V.times(postMeans.get(i)).times(diff.transpose())));
		}
		Sigma = Sigma.times(1.0/(double)N);
		//need to clear the two ArrayList
		postMeans.clear();
		postMoments.clear();
	}
	
	public Matrix post_mean(Matrix x){
		return (L.inverse()).times(V.transpose()).times(Sigma.inverse()).times(x.minus(m));
	}
	public Matrix post_moment(Matrix x){
		return (L.inverse()).plus(post_mean(x).times(post_mean(x).transpose()));
	}
	public double logLikelihood(){
		double llh = 0.0;
		double [] means;
		double [][] covariances;
		means = m.getColumnPackedCopy();
		covariances = (V.times(V.transpose())).plus(Sigma).getArray();
		MultivariateNormalDistribution mnd = new MultivariateNormalDistribution(means, covariances);
		for(int i = 0; i < N; i++)
			llh += Math.log((mnd.density(X.getMatrix(0, R-1, i, i).getColumnPackedCopy()) + 1) / 2); //apply Laplace smoothing
		return llh;
	}
	public void trainModel(int noIters){
		for(int i = 0; i < noIters; i++){
			if(i == 0)
				initE();
			else
				eStep();
			mStep();
			System.out.println(""+ (i+1) +"-th iteration");
			System.out.println("VV` matrix is :");
			(V.times(V.transpose())).print(7,3);
			System.out.println("Sigma matrix is :");
			Sigma.print(7,3);
			System.out.println("log-likelihood is "+ logLikelihood());
			System.out.println();
		}
	}
	public static void main(String[] args)  {
		/* tesing the FA training program with Gaussian distribution
		//1000 vectors of 10 dimensions - content[10][1000]
		double [] [] content = new double[10][1000];
		//two of the 10 dimensions(rows) have large variance, the rest should have small variance
		NormalDistribution largeVarianceGaussian = new NormalDistribution(0.0, 10.0);
		NormalDistribution smallVarianceGaussian = new NormalDistribution(0.0, 2.0);
		//fill the matrix with smallVarianceGaussian
		for(int i = 0; i < 10; i++)
			for(int j = 0; j < 1000; j++)
				content[i][j] = smallVarianceGaussian.sample();
		int k1 = ((int)Math.random()) % 5;	//1st large Gaussian dimension
		int k2 = k1 + 5;					//2nd large Gaussian dimension
		for(int i = 0; i < 1000; i++){
			content[k1][i] = largeVarianceGaussian.sample();
			content[k2][i] = largeVarianceGaussian.sample();
		}
		Matrix A = new Matrix(content);
		A.transpose().print(5, 3);
		FactorAnalysis fa = new FactorAnalysis(A,4);
		fa.trainModel(20);*/
	
		//test FA training program with FA distribution
		FADataGenerator dg = new FADataGenerator(60,4);
		Matrix A = dg.generateSamples(20000);
		/*FactorAnalysis fa = new FactorAnalysis(A,4);
		fa.trainModel(1000);
		System.out.println("The actual VV' matrix is : ");
		dg.V.times(dg.V.transpose()).print(7, 3);
		System.out.println("The actual Sigma matrix is : ");
		dg.Sigma.print(7, 3);*/
		PrintWriter output;
		try {
			output = new PrintWriter("data3.txt");
			A.transpose().print(output, 5, 3);
			output.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		
		
	}

}
