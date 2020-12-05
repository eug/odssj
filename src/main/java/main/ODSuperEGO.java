package main;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Random;

import common.DistanceFunction;
import common.EuclideanDistance;
import common.Instance;
import common.Table;

public class ODSuperEGO {

	private int t;
	private Table a;
	private Table b;
	private Random rand;
	private int numDim;
	private DistanceFunction dist;
	
	private double[][] range1;
	private double[][] range2;
	private double[][] range3;

	private final boolean reorderDim;
	private final boolean allowSelfSimilar;
	private boolean isSelfJoin;
	
	private ArrayList<Integer> result;
	private final int outlierThreshold;

	public ODSuperEGO(int outlierThreshold, int joinThreshold) {
		this(outlierThreshold, joinThreshold, false, false, new EuclideanDistance());
	}
	
	public ODSuperEGO(int outlierThreshold) {
		this(outlierThreshold, 1000, false, false, new EuclideanDistance());
	}
	
	public ODSuperEGO(int outlierThreshold, int joinThreshold, boolean reorderDim, boolean allowSelfSimilar) {
		this(outlierThreshold, joinThreshold, reorderDim, allowSelfSimilar, new EuclideanDistance());
	}
	
	public ODSuperEGO(int outlierThreshold, int joinThreshold, boolean reorderDim,
					boolean allowSelfSimilar, DistanceFunction fn) {
		this.outlierThreshold = outlierThreshold;
		this.reorderDim = reorderDim;
		this.allowSelfSimilar = allowSelfSimilar;
		this.dist = fn;
		this.result = new ArrayList<Integer>();
		this.t = joinThreshold;
		this.isSelfJoin = false;
		this.rand = new Random();
	}
	
	public void range(Table a, double eps) {
		isSelfJoin = true;
		range(a, a, eps);
		isSelfJoin = false;
	}
	
	public void range(Table a, Table b, double eps) {
		assert a.getNumCols() == b.getNumCols();
		assert eps > 0;
		
		reset();
		
		for (int i = 0; i < a.getNumRows(); i++)
			result.add(0);
		
		numDim = a.getNumCols();
		
		// ranges for SimpleJoin
		range1 = new double[numDim + 1][2];
		range2 = new double[numDim + 1][2];
		range3 = new double[numDim + 1][2];		

		if (isSelfJoin) {
			this.b = this.a = a.clone();
		} else {
			this.a = a.clone();
			this.b = b.clone();
		}
		
		// reorder dimension
		if (reorderDim)
			doDimensionReorder(eps);
		
		// ego-sort
		EGOSort(a, eps);
		if (!isSelfJoin)
			EGOSort(b, eps);

		// ego-join
		int startDim = 0;
		int frA = 0;
		int toA = a.getNumRows() - 1;
		int frB = 0;
		int toB = b.getNumRows() - 1;
		
		EGOJoin(frA, toA, frB, toB, startDim, eps);	
	}
	
	private void doDimensionReorder(double eps) {
		assert a.getNumCols() >= 2;
		assert b.getNumCols() >= 2;
		assert eps > 0;
		
		int sampleSize = (int) Math.ceil(a.getNumRows() * 0.2);
		int numBuckets = (int) Math.ceil(1/eps) + 1;
		
		double[][] hA = new double[numBuckets][numDim];
		double[][] hB = new double[numBuckets][numDim];
		double[] avgDist = new double[numDim];
		
		for (int i = 0; i < sampleSize; i++) {
			// get random point from A
			int maxRowsA = (int) a.getNumRows() - 1;
			int rndRowAId = nextInt(0, maxRowsA);
			double[] rndA = a.getInstance(rndRowAId).getValues();
			
			// get random point from B
			int maxRowsB = (int) b.getNumRows() - 1;
			int rndRowBId = nextInt(0, maxRowsB);
			double[] rndB = b.getInstance(rndRowBId).getValues();
			
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
		for (int i = 0; i < a.getNumRows(); i++) {
			double[] x = new double[numDim];
			for (int j = 0; j < numDim; j++)
				x[j] = a.getAt(i, j);
			for (int j = 0; j < numDim; j++)
				a.setAt(i, j, x[map[j]]);
		}
		
		// reorder dimension in A (inefficient)
		if (!isSelfJoin) {
			for (int i = 0; i < b.getNumRows(); i++) {
				double[] x = new double[numDim];
				for (int j = 0; j < numDim; j++)
					x[j] = b.getAt(i, j);
				for (int j = 0; j < numDim; j++)
					b.setAt(i, j, x[map[j]]);
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
		Collections.sort(t.asList(), new Comparator<Instance>() {
			@Override
			public int compare(Instance r1, Instance r2) {
				for (int i = 0; i < r1.getValues().length; i++) {
					int d = ((int) (r1.getAt(i)/eps)) - ((int) (r2.getAt(i)/eps));					
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

		double[] fstA = a.getInstance(frA).getValues();
		double[] lstA = a.getInstance(toA).getValues();
		double[] fstB = b.getInstance(frB).getValues();
		double[] lstB = b.getInstance(toB).getValues();
		
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
			/* A_1 join B_1 */
			EGOJoin(frA     , midA, frB     , midB	, startDim, eps);
			/* A_1 join B_2 */
			EGOJoin(frA     , midA, midB + 1, toB	, startDim, eps);
			/* A_2 join B_1 */
			EGOJoin(midA + 1, toA , frB		, midB	, startDim, eps);
			/* comment to avoid incomplete neighbor count in R */
//			if (!isSelfJoin)
				/* A_2 join B_2 */
				EGOJoin(midA + 1, toA , midB + 1, toB	, startDim, eps);
			return;
		}
	}
	
	private void NaiveJoin(int frA, int toA, int frB, int toB, double eps) {
		Instance p, q;		

		for (int i = frA; i <= toA; i++) {
			p = a.getInstance(i);
			if (result.get(p.getId()) > outlierThreshold) continue;
			
			for (int j = frB; j <= toB; j++) {
				q = b.getInstance(j);

				if (!allowSelfSimilar && (p.getId() == q.getId()))
					continue;
				
				if (dist.compute(p.getValues(), q.getValues()) <= eps) {

					/* comment to avoid twice neighbor count in R */
//					if (isSelfJoin) {
//						result.set(p.getId(), result.get(p.getId()) + 1);
//						result.set(q.getId(), result.get(q.getId()) + 1);
//					} else {
					result.set(p.getId(), result.get(p.getId()) + 1);
//					}
				}
				
				if (result.get(p.getId()) > outlierThreshold) break;
			}
		}
	}

	public void reset() {
		result.clear();
	}

	public ArrayList<Integer> getResult() {
		return result;
	}			
}