package simjoin.quickjoin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Random;

import org.apache.commons.lang3.NotImplementedException;
import org.javatuples.Pair;

import common.DistanceFunction;
import common.Row;
import common.Table;
import simjoin.RangeJoin;
import simjoin.kNNJoin;
import simjoin.collector.ResultCollector;

/*
 * This is a single thread implementation based on the following resources.
 * Only the range join operation is implemented.
 * 
 * Publication:
 * Fredriksson, K., & Braithwaite, B. (2015). Quicker range- and k-NN joins in metric spaces. 
 * Information Systems, 52, 189–204. https://doi.org/10.1016/j.is.2014.09.006
 */
public class QuickJoin implements RangeJoin {
	
	private class Indices extends ArrayList<Integer> {
		public Table table;
		public Indices(Table t) {
			this(t, 10);
		}
		public Indices(Table t, int c){
			super(c);
			table = t;
		}
	}
	
	private class Partitions {
		public final Indices L;
		public final Indices G;
		public final Indices Lw;
		public final Indices Gw;
		public Partitions(Indices L, Indices G, Indices Lw, Indices Gw) {
			this.L = L;
			this.G = G;
			this.Lw = Lw;
			this.Gw = Gw;
		}
	}
	
	private Random rand = new Random();
	private double beta = 1;
	
	private int c;
	private boolean isSelfJoin;
	private boolean isSelfSimilar;
	private DistanceFunction dist;
	private ResultCollector<?> rc;
	
	public QuickJoin(int joinThreshold, boolean selfSimilar,
					 DistanceFunction fn,
					 ResultCollector<?> rc) {
		this.c = joinThreshold;
		this.isSelfSimilar = selfSimilar;
		this.isSelfJoin = false;
		this.dist = fn;
		this.rc = rc;
	}

	@Override
	public void range(Table t, double r) {
		Indices s1 = getIndices(t);
		doQuickJoin(s1, r);
		isSelfJoin = false;
	}
	
	@Override	
	public void range(Table t1, Table t2, double r) {
		Indices s1 = getIndices(t1);
		Indices s2 = getIndices(t2);	
		doQuickJoinWin(s1, s2, r);
	}

	private Indices getIndices(Table t) {
		Indices s = new Indices(t, t.numRows);
		for (int i = 0; i < t.numRows; i++)
			s.add(t.rows[i].id);
		return s;
	}
	
	private void doQuickJoin(Indices s, double r) {
		if (s.size() < c) {
			SelfJoinBF(s, r);
			return;
		}
		
		Pair<Row, Row> pivots = Pivots(s);
		Row p1 = pivots.getValue0();
		Row p2 = pivots.getValue1();
		
		double rho = beta * dist.compute(p1.values, p2.values);
		
		Partitions part = Partition(s, p1, rho, r);
		
		doQuickJoinWin(part.Lw, part.Gw, r);
		doQuickJoin(part.L, r);
		doQuickJoin(part.G, r);
	}
	
	private void doQuickJoinWin(Indices s1, Indices s2, double r) {
		if (s1.size() + s2.size() <= c) {
			JoinBF(s1, s2, r);
			return;
		}
		
		Pair<Row, Row> pivots = Pivots(s1, s2);
		Row p1 = pivots.getValue0();
		Row p2 = pivots.getValue1();
		
		double rho = beta * dist.compute(p1.values, p2.values);
		
		Partitions part1 = Partition(s1, p1, rho, r);
		Partitions part2 = Partition(s2, p1, rho, r); // in paper says p1
		
		doQuickJoinWin(part1.Lw, part2.Gw, r);
		doQuickJoinWin(part1.Gw, part2.Lw, r);
		doQuickJoinWin(part1.L, part2.L, r);
		doQuickJoinWin(part1.L, part2.G, r);
	}
	
	private Pair<Row, Row> Pivots(Indices s) {
		int pos1 = nextInt(0, s.size() - 1);
		int pos2 = nextInt(0, s.size() - 1);
		
		while (pos1 == pos2)
			pos1 = nextInt(0, s.size() - 1);

		Row r1 = s.table.rows[s.get(pos1)];
		Row r2 = s.table.rows[s.get(pos2)];

		return new Pair<Row, Row>(r1, r2);
	}
	
	private Pair<Row, Row> Pivots(Indices s1, Indices s2) {
		Row r1 = null, r2 = null;
		int sz = s1.size() + s2.size() - 1;

		int pos1 = nextInt(0, sz);
		int pos2 = nextInt(0, sz);

		while (pos1 == pos2)
			pos1 = nextInt(0, sz);
		
		if ((pos1 < s1.size()) && (pos2 < s1.size())) {
			r1 = s1.table.rows[s1.get(pos1)];
			r2 = s2.table.rows[s1.get(pos2)];
		}
		else if ((pos1 >= s1.size()) && (pos2 >= s1.size())) {
			r1 = s2.table.rows[s2.get(pos1 - s1.size())];
			r2 = s2.table.rows[s2.get(pos2 - s1.size())];
		}
		else if ((pos1 < s1.size()) && (pos2 >= s1.size())) {
			r1 = s1.table.rows[s1.get(pos1)];
			r2 = s2.table.rows[s2.get(pos2 - s1.size())];
		}		
		else if ((pos1 >= s1.size()) && (pos2 < s1.size())) {
			r1 = s2.table.rows[s2.get(pos1 - s1.size())];
			r2 = s1.table.rows[s1.get(pos2)];
		}

		return new Pair<Row, Row>(r1, r2);
	}
	
	private Partitions Partition(Indices s, Row p, double rho, double r) {
		Row q;
		Indices L  = new Indices(s.table);
		Indices G  = new Indices(s.table);
		Indices Lw = new Indices(s.table);
		Indices Gw = new Indices(s.table);
		
		for (int i = 0; i < s.size(); i++) {
			q = s.table.rows[s.get(i)];
			if (dist.compute(q.values, p.values) < rho) {
				L.add(s.get(i));
				if ((rho - r) <= dist.compute(q.values, p.values))
					Lw.add(s.get(i));
			} else {
				G.add(s.get(i));
				if (dist.compute(q.values, p.values) <= (rho + r))
					Gw.add(s.get(i));
			}
		}
		
		return new Partitions(L, G, Lw, Gw);
	}
	
	private Partitions PartitionHoare(Indices s, Row p, double rho, double r) {
		throw new NotImplementedException("Not implemented yet");
	}
	
	private Partitions PartitionHoareOpt(Indices s, Row p, double rho, double r) {
		throw new NotImplementedException("Not implemented yet");
	}
	
	private void SelfJoinBF(Indices s, double r) {
		Row p, q; 
		for (int i = 0; i < s.size(); i++) {
			p = s.table.rows[s.get(i)];
			for (int j = i + 1; j < s.size(); j++) {
				q = s.table.rows[s.get(j)];
				if (dist.compute(p.values, q.values) <= r) {
					rc.addPair(p.id, q.id);
				}
			}
		}
	}
	
	private void JoinBF(Indices s1, Indices s2, double r) {
		Row p, q; 
		for (int i = 0; i < s1.size(); i++) {
			p = s1.table.rows[s1.get(i)];
			for (int j = 0; j < s2.size(); j++) {
				q = s2.table.rows[s2.get(j)];
				if (dist.compute(p.values, q.values) <= r) {
					rc.addPair(p.id, q.id);
				}
			}
		}
	}
	
	private void JoinPivots(Indices s1, Indices s2, double r) {
		Row p, q;
		int k = nextInt((int) 0.3 * s1.size(), (int) 0.6 * s1.size());
		double[] D = new double[k];
		double[][] P = new double[k][s2.size()];
		
		for (int i = 0; i <= k; i++) {
			for (int j = 0; j < s2.size(); j++) {
				p = s1.table.rows[s1.get(i)];
				q = s2.table.rows[s2.get(j)];
				P[i][j] = dist.compute(p.values, q.values);
				if (P[i][j] <= r) {
					rc.addPair(p.id, q.id);
				}
			}
		}
		
		for (int i = k + 1; i < s1.size(); i++) {
			for (int l = 0; l <= k; l++) {
				p = s1.table.rows[s1.get(l)];
				q = s1.table.rows[s1.get(i)]; 
				D[l] = dist.compute(p.values, q.values);
			}
			for (int j = 0; j < s2.size(); j++) {
				boolean f = false;
				for (int l = 0; l <= k; l++) {
					if (Math.abs(P[l][j] - D[l]) > r) {
						f = true;
						break;
					}
				}
				p = s1.table.rows[s1.get(i)];
				q = s2.table.rows[s2.get(j)];
				double e = dist.compute(p.values, q.values);
				if (!f && (e <= r)) {
					rc.addPair(p.id, q.id);
				}
			}
		}
	}

	private void JoinDC(Indices s1, Indices s2, double r) {
		// TODO: review implementation
		double[] dl = new double[s2.table.numRows];
		double[] du = new double[s2.table.numRows];
		
		for (int j = 0; j < s2.size(); j++) {
			dl[j] = 0;
			du[j] = Double.MAX_VALUE;
		}
		
		Row p, q;
		double e = Double.MAX_VALUE;
		for (int i = 0; i < s2.size(); i++) {
			
			if (i > 1) {
				p = s1.table.rows[s1.get(i)];
				q = s1.table.rows[s1.get(i-1)];
				e = dist.compute(p.values, q.values);
			}
			
			for (int j = 0; j < s2.size(); j++) {
				dl[j] = Math.max(Math.max(e-du[j], dl[j]-e), 0);
				if (du[j] <= r) {
					rc.addPair(s1.get(i), s2.get(j));
				} else if (dl[j] <= r) {
					p = s1.table.rows[s1.get(i)];
					q = s2.table.rows[s2.get(j)];
					dl[j] = du[j] = dist.compute(p.values, q.values);
					if (du[j] <= r) {
						rc.addPair(s1.get(i), s2.get(j));
					}
				} 
			}
		}
	}
	
	private void JoinSlidingWin(Indices s1, Indices s2, double r) {
		throw new NotImplementedException("Not implemented yet");
	}
	
	private int nextInt(int min, int max) {
		return rand.nextInt(max - min + 1) + min;
	}

	@Override
	public void reset() {
		rc.clear();
	}

	@Override
	public ResultCollector<?> getResultCollector() {
		return rc;
	}

}
