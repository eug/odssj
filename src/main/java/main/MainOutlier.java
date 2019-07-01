package main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.TreeSet;

import common.AUC;
import common.EuclideanDistance;
import common.DistanceFunction;
import common.Neighbors;
import common.Row;
import common.Table;
import outlier.Model;
import outlier.ThresholdModel;
import simjoin.collector.ArrayRC;
import simjoin.collector.HashMapRC;
import simjoin.superego.SuperEGO;

public class MainOutlier {
	
	public static double t = 0;

	public static void main(String[] args) {
//		Table glass = Table.readCSV("datasets/glass.csv", ",", 9, true);
//		Table X = glass;
		
//		Table wine = Table.readCSV("datasets/wine.csv", ",", 13, true);
//		Table X = wine;
		
//		Table thyroid = Table.readCSV("datasets/thyroid.csv", ",", 6, true);
//		Table X = thyroid;
		
		int[] columns = {
//				9,
//				7,
//				9,
//				20,
//				22,
//				36,
				9,
//				6,
//				13,
//				3
		};
		
		String[] datasets = {
//				"breastw",
//				"ecoli",
//				"glass", 
//				"hepatitis", 
//				"parkinson", 
//				"satimage-2", 
				"shuttle", 
//				"thyroid", 
//				"wine", 
//				"http"
		};

		for (int i = 0; i < datasets.length; i++) {
			
			Table X = Table.readCSV("datasets/" + datasets[i] +".csv", ",", columns[i], true);
			X.normalize();
			
			int bestC = -1;
			double bestEPS = -1;
			double bestTime = -1;
			double bestAUC = Double.MIN_VALUE;
			
			double[] output = new double[X.numRows];
			
			System.out.println(String.format("dataset\t%s", datasets[i]));
			
			for (int c = 0; c < 3000; c+=100) {			
				Model M = new ThresholdModel(c);
				
				for (double r = 0.001; r < 0.5; r+=0.001) {
					Runtime.getRuntime().gc();
					t = 0;
					Neighbors N = SSJ(X, r, 10);
					long start = System.currentTimeMillis();
					for (int k = 0; k < N.size(); k++)
						output[k] = M.flag(X, N, k);
					long end = System.currentTimeMillis();
					t += (end - start)/1000.0;

					int[] truth = getLabels(X);
					double auc = AUC.measure(truth, output);

					if (auc > bestAUC) {
						bestAUC = auc;
						bestC = c;
						bestEPS = r;
						bestTime = t;
						
						System.out.println(String.format("c\t%d", bestC));
						System.out.println(String.format("eps\t%.3f", bestEPS));
						System.out.println(String.format("rocauc\t%.2f", bestAUC));
						System.out.println(String.format("time\t%.2f\n", bestTime));
					}
				}
			}

			System.out.println("========== Best Result ==========");
			System.out.println(String.format("dataset\t%s", datasets[i]));
			System.out.println(String.format("c\t%d", bestC));
			System.out.println(String.format("eps\t%.3f", bestEPS));
			System.out.println(String.format("rocauc\t%.2f", bestAUC));
			System.out.println(String.format("time\t%.2f\n", bestTime));
			System.out.println("=================================");
		}
		
	}

	private static int[] getLabels(Table X) {
		int[] labels = new int[X.numRows];
		for (int i = 0; i < X.numRows; i++) {
			labels[i] = X.rows[i].label;
		}
		return labels;
	}

	private static Neighbors SSJ(Table X, double r, int joinThreshold) {
//		HashMapRC rc = new HashMapRC();
		ArrayRC rc = new ArrayRC();
		DistanceFunction dist = new EuclideanDistance();
		SuperEGO superEgo = new SuperEGO(joinThreshold, false, false, dist, rc);
		
		long start = System.currentTimeMillis();
		superEgo.range(X, r);
		long end = System.currentTimeMillis();
		t += (end - start)/1000.0;

		ArrayList<Integer> result = 
    			(ArrayList<Integer>) superEgo.getResultCollector().getResult();
			
    	Neighbors N = new Neighbors(X.numRows);
    	for (int i = 0; i < X.numRows; i++)
    		N.add(new ArrayList<>());
  	
    	for (int i = 0, j = 1; j < result.size(); i++, j++) {
    		N.get(result.get(i)).add(result.get(j));
    	}
		
		return N;
	}

}
