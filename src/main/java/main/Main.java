package main;

import common.Table;

public class Main {

	public static void main(String[] args) {
		Table breastw = Table.readCSV("datasets/breastw.csv", ",", 9, true);
		Table ecoli = Table.readCSV("datasets/ecoli.csv", ",", 7, true);
		Table glass = Table.readCSV("datasets/glass.csv", ",", 9, true);
		Table hepatitis = Table.readCSV("datasets/hepatitis.csv", ",", 20, true);
		Table parkinson = Table.readCSV("datasets/parkinson.csv", ",", 22, true);
		Table satimage2 = Table.readCSV("datasets/satimage-2.csv", ",", 36, true);
		Table shuttle = Table.readCSV("datasets/shuttle.csv", ",", 9, true);
		Table thyroid = Table.readCSV("datasets/thyroid.csv", ",", 6, true);
		Table wine = Table.readCSV("datasets/wine.csv", ",", 13, true);
		Table http = Table.readCSV("datasets/http.csv", ",", 3, true);
	}

}
