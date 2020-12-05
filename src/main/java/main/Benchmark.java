package main;

import java.util.ArrayList;

import org.javatuples.Quartet;

import common.AUC;
import common.Table;

public class Benchmark {
	
	public static void main(String[] args) {

		ArrayList<Table> datasets = new ArrayList<>();
		datasets.add(Table.readCSV("datasets/parkinson.csv",   ",", 22, true));
		datasets.add(Table.readCSV("datasets/hepatitis.csv",   ",", 20, true));
		datasets.add(Table.readCSV("datasets/glass.csv",       ",",  9, true));
		datasets.add(Table.readCSV("datasets/ecoli.csv",       ",",  7, true));
		datasets.add(Table.readCSV("datasets/ionosphere.csv",  ",", 33, true));
		datasets.add(Table.readCSV("datasets/breastw.csv",     ",",  9, true));
		datasets.add(Table.readCSV("datasets/pima.csv",        ",",  8, true));
		datasets.add(Table.readCSV("datasets/thyroid.csv",     ",",  6, true));
		datasets.add(Table.readCSV("datasets/satimage-2.csv",  ",", 36, true));
		datasets.add(Table.readCSV("datasets/mammography.csv", ",",  6, true));
		datasets.add(Table.readCSV("datasets/shuttle.csv",     ",",  9, true));
		datasets.add(Table.readCSV("datasets/http.csv",        ",",  3, true));
		
		ArrayList<Quartet<String, Double, Integer, Integer>> parameters = new ArrayList<>();
		parameters.add(new Quartet<>("parkinson  ", 0.7482,    1,  10));
		parameters.add(new Quartet<>("hepatitis  ", 0.5190,    1,  10));
		parameters.add(new Quartet<>("glass      ", 0.0758,    4,  10));
		parameters.add(new Quartet<>("ecoli      ", 0.1869,    7,  10));
		parameters.add(new Quartet<>("ionosphere ", 0.6266,    2,  90));
		parameters.add(new Quartet<>("breastw    ", 0.2346,    7,  10));
		parameters.add(new Quartet<>("pima       ", 0.5410,   54,  10));
		parameters.add(new Quartet<>("thyroid    ", 0.0001,    1,  10));
		parameters.add(new Quartet<>("satimage-2 ", 0.7657,   34, 100));
		parameters.add(new Quartet<>("mammography", 0.0130,   65, 100));
		parameters.add(new Quartet<>("shuttle    ", 0.1266, 1230, 100));
		parameters.add(new Quartet<>("http       ", 0.2270, 1300, 100));

		assert datasets.size() == parameters.size();
		
		for (int i = 0; i < datasets.size(); i++) {		
			
			// load dataset
			Table X = datasets.get(i);
			
			// get labels
			int[] labels = getLabels(X);
			
			// normalize dataset
			X.normalize();
		
			String dataset = parameters.get(i).getValue0();
			double r = parameters.get(i).getValue1();
			int outThreshold = parameters.get(i).getValue2();
			int joinThreshold = parameters.get(i).getValue3();
			
			long start = System.currentTimeMillis();
			double[] output = SSJ(X, r, outThreshold, joinThreshold);
			long end = System.currentTimeMillis();
			double runtime = (end - start) / 1000.0;
			double auc = AUC.measure(labels, output);			
			
			System.out.println(String.format("dataset=%s\tjoinThs=%d\toutThs=%d\trange=%.4f\tauc=%.4f\truntime=%.2f",
					dataset, joinThreshold, outThreshold, r, auc, runtime));
		}
		
	}

	private static int[] getLabels(Table X) {
		int[] labels = new int[X.getNumRows()];
		for (int i = 0; i < X.getNumRows(); i++) {
			labels[i] = X.getInstance(i).getLabel();
		}
		return labels;
	}
	
	private static double[] SSJ(Table X, double radius, int threshold, int j) {
		
		ODSuperEGO superEgo = new ODSuperEGO(threshold, j);
		superEgo.range(X, radius);	
		
		double[] predict = new double[X.getNumRows()];
		for (int i = 0; i < predict.length; i++) {
			predict[i] = superEgo.getResult().get(i) < threshold ? 1 : 0;
		}

		return predict;
	}
}
