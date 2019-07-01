package simjoin.quickjoin;

import java.util.ArrayList;

import common.Table;

public class Indices extends ArrayList<Integer> {

	public Table table;
	public Indices(Table t) {
		this(t, 10);
	}
	public Indices(Table t, int c){
		super(c);
		table = t;
	}
}