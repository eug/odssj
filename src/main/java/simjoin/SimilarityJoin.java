package simjoin;

import simjoin.collector.ResultCollector;

public interface SimilarityJoin {
	public void reset();
	public ResultCollector<?> getResultCollector();
}
