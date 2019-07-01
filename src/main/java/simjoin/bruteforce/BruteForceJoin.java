package simjoin.bruteforce;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Objects;
import java.util.PriorityQueue;

import org.javatuples.Pair;

import common.DistanceFunction;
import common.Row;
import common.Table;
import simjoin.RangeJoin;
import simjoin.kNNJoin;
import simjoin.collector.ResultCollector;

public class BruteForceJoin implements RangeJoin, kNNJoin {
	
	private boolean isSelfJoin;
	private DistanceFunction dist;
	private boolean allowSelfSimilar;
	private ResultCollector<?> rc;
	
	public BruteForceJoin(boolean allowSelfSimilar, DistanceFunction fn, ResultCollector<?> rc) {
		this.isSelfJoin = false;
		this.dist = fn;
		this.allowSelfSimilar = allowSelfSimilar;
		this.rc = rc;
	}
	
	@Override
	public ResultCollector<?> getResultCollector() {
		return rc;
	}

	@Override
	public void range(Table t1, double r) {
		isSelfJoin = true;
		range(t1, t1, r);
		isSelfJoin = false;
	}

	@Override
	public void range(Table t1, Table t2, double r) {
		assert Objects.nonNull(t1);
		assert Objects.nonNull(t2);
		assert t1.numRows > 1;
		assert t2.numRows > 1;
		assert r > 0;
		
		reset();
		Row p, q;

		for (int i = 0; i < t1.numRows; i++ ) {
			p =  t1.rows[i];
			
			for (int j = 0; j < t2.numRows; j++) {
				if (!allowSelfSimilar && i == j) continue;				
				q =  t2.rows[j];
				
				if (dist.compute(p.values, q.values) <= r) {
					rc.addPair(p.id, q.id);
				}
			}
		}

	}

	@Override
	public void kNN(Table t1, int k) {
		isSelfJoin = true;
		kNN(t1, t1, k);
	}

	@Override
	public void kNN(Table t1, Table t2, int k) {
		assert Objects.nonNull(t1);
		assert Objects.nonNull(t2);
		assert t1.numRows > 1;
		assert t2.numRows > 1;
		assert k > 0;
		
		reset();
		Row p, q, r;
		
		HashMap<Integer, PriorityQueue<Pair<Double, Integer>>> R = new HashMap<>();
		
		for (int i = 0; i < t1.numRows; i++ ) {
			p = t1.rows[i];

			for (int j = 0; j < t2.numRows; j++) {
				q = t2.rows[j];
			
				if (!allowSelfSimilar && p.id == q.id)
					continue;
				
				if (!R.containsKey(p.id)) {					
					PriorityQueue<Pair<Double, Integer>> pq;
					pq = new PriorityQueue<>(k + 1, Collections.reverseOrder()); 
					R.put(i, pq);
				}
				
				Double pqDist = dist.compute(p.values, q.values);
				PriorityQueue<Pair<Double, Integer>> neighbors = R.get(p.id);
				
				if (neighbors.size() < k) {
					neighbors.add(new Pair<>(pqDist, q.id));
					continue;
				}

				// else find max dist in p neighborhood
				Pair<Double, Integer> mostDistantIdx = neighbors.peek();
				if (pqDist < mostDistantIdx.getValue0()) {
					neighbors.poll();
					neighbors.add(new Pair<>(pqDist, q.id));
				}
			}
		}
		
		for (Integer idx1 : R.keySet()) {
			for (Pair<Double, Integer> distIdx : R.get(idx1)) {
				rc.addPair(idx1, distIdx.getValue1());
			}
		}
	}

	@Override
	public void reset() {
		rc.clear();
	}

}
