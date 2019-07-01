package main;

import java.util.HashMap;
import java.util.Objects;
import java.util.TreeSet;

import common.DistanceFunction;
import common.EuclideanDistance;
import common.Row;
import common.Table;
import simjoin.bruteforce.BruteForceJoin;
import simjoin.collector.HashMapRC;

public class MainBFJoin {
	public static void main(String[] args) {
//		Table http = Table.readCSV("datasets/http.csv", ",", 3, true);
//		Table table = http;
		
		Table glass = Table.readCSV("datasets/glass.csv", ",", 9, true);
		Table table = glass;

//		Table thyroid = Table.readCSV("datasets/thyroid.csv", ",", 6, true);
//		Table table = thyroid;

		table.normalize();

		HashMapRC rc = new HashMapRC();
		DistanceFunction dist = new EuclideanDistance();
		BruteForceJoin bfjoin = new BruteForceJoin(false, dist, rc);

		bfjoin.range(table, 0.01);
		
		HashMap<Integer, TreeSet<Integer>> result = 
				(HashMap<Integer, TreeSet<Integer>>) bfjoin.getResultCollector().getResult();

//		for (Row row : table.rows) {
//			TreeSet<Integer> neighbors = result.get(row.id);
//			if (row.label == 1) {
//				if (!Objects.isNull(neighbors)) {
//					System.out.println(String.format("[%d]\tId=%d\t(%d)", row.id, row.label,  neighbors.size()));
//				} else {
//					System.out.println(String.format("[%d]\tId=%d\t(0)", row.id, row.label));
//				}
//			}
//		}

		for (Row row : table.rows) {
			TreeSet<Integer> neighbors = result.get(row.id);
			if (!Objects.isNull(neighbors) && neighbors.size() < 5 ) {
				System.out.println(String.format("[%d]\tId=%d\t(%d)", row.id, row.label,  neighbors.size()));
			} else {
				System.out.println(String.format("[%d]\tId=%d\t(0)", row.id, row.label));
			}
		}
		
	}
}
