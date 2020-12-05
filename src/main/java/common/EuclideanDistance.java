package common;

public class EuclideanDistance implements DistanceFunction {

	@Override
	public double compute(double[] p, double[] q) {
		return new org.apache.commons.math3.ml.distance.EuclideanDistance().compute(p, q);
	}

}
