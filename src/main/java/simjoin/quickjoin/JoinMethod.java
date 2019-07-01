package simjoin.quickjoin;

import simjoin.collector.ResultCollector;

public interface JoinMethod {
	public void rangeJoin(ResultCollector<?> rc, Indices s, double r);
	public void rangeJoin(ResultCollector<?> rc, Indices s1, Indices s2, double r);
	public void kNNJoin(ResultCollector<?> rc, Indices s, int k);
	public void kNNJoin(ResultCollector<?> rc, Indices s1, Indices s2, int k);
}
