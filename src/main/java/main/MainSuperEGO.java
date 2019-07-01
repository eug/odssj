package main;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Objects;
import java.util.TreeSet;

import common.DistanceFunction;
import common.EuclideanDistance;
import common.Row;
import common.Table;
import simjoin.collector.HashMapRC;
import simjoin.quickjoin.QuickJoin;
import simjoin.superego.SuperEGO;

public class MainSuperEGO {

    public static void main( String[] args ) throws IOException{   	
    	Table table = Table.readCSV("/home/eugfc/Downloads/Glass/GlassNoLabel.csv", ",", -1, true);
    	
		HashMapRC rc = new HashMapRC();
		DistanceFunction dist = new EuclideanDistance();
    	SuperEGO sejoin = new SuperEGO(100, false, false, dist, rc);
    	
    	sejoin.range(table, 0.05);
    	
    	HashMap<Integer, TreeSet<Integer>> result = 
    			(HashMap<Integer, TreeSet<Integer>>) sejoin.getResultCollector().getResult();
    	
		for (Row row : table.rows) {
			Integer idx1 = row.id;
			System.out.print(String.format("SuperEGO [%d] => ", idx1));
			TreeSet<Integer> neighbors = result.get(idx1);
			if (!Objects.isNull(neighbors)) {
				for (Integer idx2 : neighbors) {
					double d = dist.compute(table.rows[idx1].values, table.rows[idx2].values);
					System.out.print(String.format("(%.2f, %d) ", d, idx2));					
				}
			}
			System.out.println();
		}

    }
}
