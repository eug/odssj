package simjoin;

import common.Table;

public interface kNNJoin extends SimilarityJoin {
	public void kNN(Table t1, int k);
	public void kNN(Table t1, Table t2, int k);
}
