/*
 * Author: Man-Wai MAK, Dept. of EIE, The Hong Kong Polytechnic University
 * Version: 1.0
 * Date: March 2015
 * 
 * This file is subject to the terms and conditions defined in
 * file 'license.txt', which is part of this source code package.
*/

package parallel.gmm;

public class Config {
	public static final int NUM_MIX = 16;			// Must match nMix in run_GMM.sh
	public static final int DIM = 60;				// Must match D in matlab/generate_data.m
	public static final String nameNode = "hdfs://master:50000";
	public static final String GMM_FILE = nameNode + "/user/mwmak/stats/gmm.txt";
}
