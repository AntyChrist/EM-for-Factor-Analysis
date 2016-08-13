/*
 * Author: LIU Sining, 12132473D, The Hong Kong Polytechnic University
 * Version: 2.0
 * Date: July 2016
 * 
 * This file is subject to the terms and conditions defined in
 * file 'license.txt', which is part of this source code package.
*/

package parallel.fa;

public class Config {			
	public static final int DIM = 60;					
	public static final int NO_LATENT_FACTOR = 4;
	public static final String nameNode = "hdfs://master:50000";
	public static final String FA_FILE = nameNode + "/user/liusining/stats/fa.txt";
}
