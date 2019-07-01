package simjoin.collector;

import java.util.Collection;
import java.util.HashMap;
import java.util.TreeSet;

public class HashMapRC implements ResultCollector<HashMap<Integer, TreeSet<Integer>>> {
	
	private HashMap<Integer, TreeSet<Integer>> R = new HashMap<Integer, TreeSet<Integer>>();
	
	@Override
	public HashMap<Integer, TreeSet<Integer>> getResult() {
		return R;
	}

	@Override
	public void addPair(Integer idx1, Integer idx2) {
		if (!R.containsKey(idx1))
			R.put(idx1, new TreeSet<Integer>());
		R.get(idx1).add(idx2);
	}

	@Override
	public Collection<Integer> getPairsOf(Integer idx) {
		return R.get(idx);
	}

	@Override
	public void clear() {
		R.clear();
	}

}
