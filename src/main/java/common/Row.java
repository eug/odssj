package common;

public class Row  {
	public int id;
	public double[] values;
	public int label;
	public boolean hasLabel;
	
	public Row(int id, double[] values) {
		this(id, values, -1);
	}

	public Row(int id, double[] values, int labelValue) {
		hasLabel = false;
		if (labelValue >= 0)
			hasLabel = true;
		this.label = labelValue;
		this.id = id;
		this.values = new double[values.length];
		for (int i = 0; i < values.length; i++)
			this.values[i] = values[i];
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(String.format("(%3s, ", this.id));
		for(int i = 0; i < values.length; i++) {
			str.append(String.format("%.3f ", values[i]));
		}
		str.append(")");
		return str.toString();
	}
	
}
