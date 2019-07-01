package simjoin.collector;

import java.util.Collection;

public interface ResultCollector<T> {
	public T getResult();
	public void addPair(Integer idx1, Integer idx2);
	public Collection<Integer> getPairsOf(Integer idx);
	public void clear();
}
