package simjoin.superego;

import java.util.Collections;
import java.util.Comparator;
import java.util.Random;

import common.DistanceFunction;
import common.EuclideanDistance;
import common.Row;
import common.Table;
import simjoin.RangeJoin;
import simjoin.collector.ResultCollector;

/*
 * This is a single thread implementation based on the following resources.
 * 
 * Source Code:
 * https://www.ics.uci.edu/~dvk/code/SuperEGO.html
 * 
 * Publication:
 * Kalashnikov, D. V. (2013). Super-EGO: Fast Multi-dimensional Similarity Join.
 * VLDB Journal, 22(4), 561–585. https://doi.org/10.1007/s00778-012-0305-7 
 */
public class SuperEGO implements RangeJoin {

	private int t;
	private Table table1;
	private Table table2;
	private Random rand = new Random();
	private int numDim;
	private DistanceFunction dist = new EuclideanDistance();
	
	private double[][] range1;
	private double[][] range2;
	private double[][] range3;

	private final boolean reorderDim;
	private final boolean allowSelfSimilar;
	private boolean isSelfJoin;
	private ResultCollector<?> rc;
	
	public SuperEGO(int joinThreshold, boolean reorderDim, boolean allowSelfSimilar,
					DistanceFunction fn, ResultCollector<?> rc) {
		this.reorderDim = reorderDim;
		this.allowSelfSimilar = allowSelfSimilar;
		this.dist = fn;
		this.rc = rc;
		this.t = joinThreshold;
		this.isSelfJoin = false;
	}
	
	@Override
	public void range(Table a, double eps) {
		isSelfJoin = true;
		range(a, a, eps);
		isSelfJoin = false;
	}
	
	@Override
	public void range(Table a, Table b, double eps) {
		assert a.numCols == b.numCols;
		assert eps > 0;
		
		reset();
		
		numDim = a.numCols;
		
		// ranges for SimpleJoin
		range1 = new double[numDim + 1][2];
		range2 = new double[numDim + 1][2];
		range3 = new double[numDim + 1][2];		

		if (isSelfJoin) {
			table2 = table1 = a.clone();
		} else {
			table1 = a.clone();
			table2 = b.clone();
		}
		
		// reorder dimension
		if (reorderDim)
			doDimensionReorder(eps);
		
		// ego-sort
		EGOSort(table1, eps);
		if (!isSelfJoin)
			EGOSort(table2, eps);

		// ego-join
		int startDim = 0;
		int frA = 0;
		int toA = table1.numRows - 1;
		int frB = 0;
		int toB = table2.numRows - 1;
		
		EGOJoin(frA, toA, frB, toB, startDim, eps);	
	}
	
	private void doDimensionReorder(double eps) {
		assert table1.numCols >= 2;
		assert table2.numCols >= 2;
		assert eps > 0;
		
		int sampleSize = (int) Math.ceil(table1.numRows * 0.2);
		int numBuckets = (int) Math.ceil(1/eps) + 1;
		
		double[][] hA = new double[numBuckets][numDim];
		double[][] hB = new double[numBuckets][numDim];
		double[] avgDist = new double[numDim];
		
		for (int i = 0; i < sampleSize; i++) {
			// get random point from A
			double[] rndA = table1.rows[nextInt(0, (int)table1.numRows-1)].values;
			
			// get random point from B
			double[] rndB = table2.rows[nextInt(0, (int)table2.numRows-1)].values;
			
			// update stats
			for (int j = 0; j < numDim; j++) {
				avgDist[j] += Math.abs(rndA[j] - rndB[j]);
				int bucket_a = (int) (rndA[j]/eps);
				int bucket_b = (int) (rndB[j]/eps);
				hA[bucket_a][j] += 1;
				hB[bucket_b][j] += 1;
			}
		}
		
		// compute fail factor f in each dimension
		double[] f = new double[numDim];
		double[] g = new double[numDim];
		
		for (int i = 0; i < numDim; i++) {
			f[i] = 0;
			for (int j = 0; j < numBuckets; j++) {
				if (j == 0) {
					f[i] += hA[j][i] * (hB[j][i] + hB[j+1][i]); // assumes at least two columns (obviously)
					continue;
				} else if (j == (numBuckets - 1)) {
					f[i] += hA[j][i] * (hB[j][i] + hB[j-1][i]);
					continue;
				}
				
				f[i] += hA[j][i] * (hB[j][i] + hB[j-1][i] +hB[j+1][i]);
			}
		}
		
		// normalizing f
		for (int i = 0; i < numDim; i++) {
			avgDist[i] = avgDist[i] / sampleSize;
			f[i] = f[i]/ (sampleSize * sampleSize);
			g[i] = -avgDist[i];
		}
		
		int[] map = new int[numDim];
		
		// constracting map of remapping (inefficient)
		for (int i = 0; i < numDim; i++) {
			double min = Double.MAX_VALUE;
			int min_idx = -1;
			for (int j = 0; j < numDim; j++) {
				if (g[j] < min) {
					min = g[j];
					min_idx = j;
				}
			}
			map[i] = min_idx;
			g[min_idx] = Double.MAX_VALUE;
		}
		
		
		// reorder dimension in A (inefficient)
		for (int i = 0; i < table1.numRows; i++) {
			double[] x = new double[numDim];
			for (int j = 0; j < numDim; j++)
				x[j] = table1.rows[i].values[j];
			for (int j = 0; j < numDim; j++)
				table1.rows[i].values[j] = x[map[j]];
		}
		
		// reorder dimension in A (inefficient)
		if (!isSelfJoin) {
			for (int i = 0; i < table2.numRows; i++) {
				double[] x = new double[numDim];
				for (int j = 0; j < numDim; j++)
					x[j] = table2.rows[i].values[j];
				for (int j = 0; j < numDim; j++)
					table2.rows[i].values[j] = x[map[j]];
			}
		}
		
		// reorder stats accordingly
		double[] rs = new double[numDim];
		double[] rd = new double[numDim];
		for(int j = 0; j < numDim; j++) {
			rs[j] = 1 - f[map[j]];
			rd[j] = avgDist[map[j]];
		}
		
		
		//-- Case 1: zero inactive dimensions --
		int smallSeqSize = Math.min(1, numDim);
		for (int i = 0; i< smallSeqSize; i++) {
			range1[i][0] = 0;
			range1[i][1] = numDim - 1;
			range2[i][0] = 0;
			range2[i][1] = -1;
			range3[i][0] = 0;
			range3[i][1] = -1;
		}

		//-- Case 2: all dims are inactive --
		range1[numDim][0] = 0;
		range1[numDim][1] = numDim - 1;
		range2[numDim][0] = 0;
		range2[numDim][1] = -1;
		range3[numDim][0] = 0;
		range3[numDim][1] = -1;
	    
		//-- Case 3: remaining cases --
	    //-- find first k s.t. rd[k] < eps/2 --
	    int k = numDim;
	    
	    for (int i = 0; i < numDim; i++) {
	        if (rd[i] < eps/2) {
	            k = i;
	            break;
	        }
	    }
	    
	    for (int i = smallSeqSize; i < numDim; i++) {
	    	//-- Case I: 1-interval --
	    	if (rd[i] < eps/2) {
				range1[i][0] = 0;
				range1[i][1] = numDim - 1;
				range2[i][0] = 0;
				range2[i][1] = -1;
				range3[i][0] = 0;
				range3[i][1] = -1;
				continue;
	    	}
	    	//-- Case II: 3-intervals --
	    	if (k < numDim) {
				range1[i][0] = i;
				range1[i][1] = k - 1;
				range2[i][0] = 0;
				range2[i][1] = i - 1;
				range3[i][0] = k;
				range3[i][1] = numDim - 1;
				continue;
	    	}
	    	//-- Case III: 2-interval --
			range1[i][0] = i;
			range1[i][1] = numDim - 1;
			range2[i][0] = 0;
			range2[i][1] = i - 1;
			range3[i][0] = 0;
			range3[i][1] = -1;
	    }
	}
	
	private int nextInt(int min, int max) {
		return rand.nextInt(max - min + 1) + min;
	}
	
	private void EGOSort(Table t, double eps) {
		Collections.sort(t.asList(), new Comparator<Row>() {
			@Override
			public int compare(Row r1, Row r2) {
				for (int i = 0; i < r1.values.length; i++) {
					int d = ((int) (r1.values[i]/eps)) - ((int) (r2.values[i]/eps));					
					if (d != 0)
						return d;
				}
				return 0;
			}
		});
	}
	
	private void EGOJoin(int frA, int toA, int frB, int toB, int startDim, double eps) {
		int szA = toA - frA + 1; 
		int szB = toB - frB + 1;

		double[] fstA = table1.rows[frA].values;
		double[] lstA = table1.rows[toA].values;
		double[] fstB = table2.rows[frB].values;
		double[] lstB = table2.rows[toB].values;
		
		// Ego-Strategy
		double loA, hiA, loB, hiB;
		for (int i = startDim; i < numDim; i++) {
			loA = (int) (fstA[i] / eps);
			hiB = (int) (lstB[i] / eps);
			if (loA > hiB + 1) return;
			loB = (int) (fstB[i] / eps);
			hiA = (int) (lstA[i] / eps);
			if (loB > hiA + 1) return;
			if ((loA < hiA) || (loB < hiB)) {
				startDim = i;
				break;
			}
		}
		
		// Ego-Join
		int midA = (int) (frA + (szA/2.0));
		int midB = (int) (frB + (szB/2.0));
		
		if ((szA < t) && (szB < t)) {
			NaiveJoin(frA, toA, frB, toB, eps);
			return;
		}

		if ((szA < t) && (szB >= t)) {
			EGOJoin(frA, toA, frB     , midB, startDim, eps);
			EGOJoin(frA, toA, midB + 1,  toB, startDim, eps);
			return;
		}
		
		if ((szA >= t) && (szB < t)) {
			EGOJoin(frA     , midA, frB, toB, startDim, eps);
			EGOJoin(midA + 1, toA , frB, toB, startDim, eps);
			return;
		}
		
		if ((szA >= t) && (szB >= t)) {
			EGOJoin(frA     , midA, frB     , midB, startDim, eps);
			EGOJoin(frA     , midA, midB + 1, toB , startDim, eps);
			EGOJoin(midA + 1, toA , midB + 1, toB , startDim, eps);
			if (!isSelfJoin)
				EGOJoin(midA + 1, toA , frB     , midB, startDim, eps);
			return;
		}
	}
	
	private void NaiveJoin(int frA, int toA, int frB, int toB, double eps) {
		Row p, q;		

		for (int i = frA; i <= toA; i++) {
			p = table1.rows[i];
			for(int j = frB; j <= toB; j++) {
				q = table2.rows[j];
				
				if (!allowSelfSimilar && (p.id == q.id))
					continue;
				
				if (dist.compute(p.values, q.values) <= eps) {
					if (isSelfJoin) {
						rc.addPair(p.id, q.id);
						rc.addPair(q.id, p.id);
					} else {
						rc.addPair(p.id, q.id);						
					}
				}
			}
		}
	}
	
	private void SimpleJoin(int frA, int toA, int frB, int toB, double eps) {
		Row p, q;
		double eps2 = eps * eps;
		double s, dx;
		boolean skip;
		
		for (int i = frA; i <= toA; i++) {
			p = table1.rows[i];

			for (int j = frB; j <= toB; j++) {
				q = table2.rows[j];
				
				if (!allowSelfSimilar && (p.id == q.id))
					continue;
				
				s = 0;
				dx = 0;
				skip = false;
				
				for (int k = 0; k < numDim; k++) {
					dx = p.values[k] - q.values[k];
					s += dx * dx;
					if (s >= eps2) {
						skip = true;
						break;
					}
				}
				
				if (!skip) {
					if (isSelfJoin) {
						rc.addPair(p.id, q.id);
						rc.addPair(q.id, p.id);
					} else {
						rc.addPair(p.id, q.id);						
					}					
				}
				
				skip = false;
			}
		}
	}
	
	private void  SimpleJoinAlternative(int frA, int toA, int frB, int toB, double eps) {
		Row p, q;
		double s;
		double dx;
		boolean skip;
		final double eps2 = eps * eps;

		for (int i = frA; i <= toA; i++) {
			p = table1.rows[i];
			for (int j = frB; j <= toB; j++) {
				q = table2.rows[j];
				s = 0;
				skip = false;
				int mid = (int) Math.floor(numDim/2);
				
				for (int k = mid; !skip && k < numDim; k++) {
					dx = p.values[k] - q.values[k];
					s += dx * dx;
					if (s >= eps2) {						
						skip = true;
						break;
					}
				}
				
				for (int k = 0; !skip && k < mid; k++) {
					dx = p.values[k] - q.values[k];
					s += dx * dx;
					if (s >= eps2) {						
						skip = true;
						break;
					}
				}
				
				if (!skip) {
					if (isSelfJoin) {
						rc.addPair(p.id, q.id);
						rc.addPair(q.id, p.id);
					} else {
						rc.addPair(p.id, q.id);						
					}
				}
				
				skip = false;
			}
		}
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
