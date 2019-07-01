package simjoin;

import common.Table;

public interface RangeJoin extends SimilarityJoin {
	public void range(Table t1, double r);
	public void range(Table t1, Table t2, double r);
}
