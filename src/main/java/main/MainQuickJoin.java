package main;

import java.io.IOException;
import java.util.HashMap;
import java.util.Objects;
import java.util.TreeSet;

import common.DistanceFunction;
import common.EuclideanDistance;
import common.Row;
import common.Table;
import simjoin.collector.HashMapRC;
import simjoin.quickjoin.QuickJoin;

public class MainQuickJoin {
    public static void main( String[] args ) throws IOException{   	
    	Table table = Table.readCSV("/home/eugfc/Downloads/Glass/GlassNoLabel.csv", ",");
    	
		HashMapRC rc = new HashMapRC();
		DistanceFunction dist = new EuclideanDistance();
    	QuickJoin qjoin = new QuickJoin(300, true, dist, rc);
    	
    	qjoin.range(table, 0.05);
    	
    	HashMap<Integer, TreeSet<Integer>> result = 
    			(HashMap<Integer, TreeSet<Integer>>) qjoin.getResultCollector();
    	
		for (Row row : table.rows) {
			Integer idx1 = row.id;
			System.out.print(String.format("QuickJoin [%d] => ", idx1));
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
