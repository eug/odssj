package simjoin.collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class ArrayRC implements ResultCollector<ArrayList<Integer>> {

	private ArrayList<Integer> R = new ArrayList<>();
	
	@Override
	public ArrayList<Integer> getResult() {
		return R;
	}

	@Override
	public void addPair(Integer idx1, Integer idx2) {
		R.add(idx1);
		R.add(idx2);
	}

	@Override
	public Collection<Integer> getPairsOf(Integer idx) {
		ArrayList<Integer> pairs = new ArrayList<>();
		Iterator<Integer> iter = R.iterator();
		while (iter.hasNext()) {
			Integer v = iter.next();
			if (v.equals(idx)) {
				pairs.add(iter.next());
			}
		}
		return pairs;
	}

	@Override
	public void clear() {
		R.clear();
	}

}
