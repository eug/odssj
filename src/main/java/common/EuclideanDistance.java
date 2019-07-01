package common;

public class EuclideanDistance implements DistanceFunction {

	@Override
	public double compute(double[] p, double[] q) {
		double v, sum = 0;
		for (int i = 0; i < p.length; i++) {
			v = (q[i] - p[i]);
			sum += v * v;
		}
		return sum;
	}

}
