package common;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;


public class Table implements Cloneable {

	public final int numRows;
	public final int numCols;
	public final Row[] rows;
	
	public Table(int numRows, int numCols, Row[] rows) {
		this.numRows = numRows;
		this.numCols = numCols;
		this.rows = rows;
	}
	
	public static Table readCSV(String filename) {
		return readCSV(filename, ",");
	}
	
	public static Table readCSV(String filename, String sep) {
		return readCSV(filename, sep, -1, false);
	}
	
	public static Table readCSV(String filename, String sep, int labelCol, boolean hasHeader) {
		int nRows = getNumRows(filename);
		int nCols = getNumCols(filename, sep);
		int nDataCols = nCols;
		
		if (hasHeader)
			nRows -= 1;
		
		if (labelCol >= 0)
			nDataCols -= 1;

		Row[] rows = new Row[nRows];
		
		try (BufferedReader bf = new BufferedReader(new FileReader(filename))) {
			String line;
			String[] v;
			int labelValue = -1;
			
			if (hasHeader)
				bf.readLine(); // skip header
			
			for (int i = 0; (line = bf.readLine()) != null; i++) {
				v = line.split(sep);
				double[] values = new double[nDataCols];

				for (int j = 0; j < v.length; j++) {
					if (j == labelCol) {
						labelValue = Integer.parseInt(v[j]);
					} else {						
						values[j] = Double.parseDouble(v[j]);
					}
				}
				
				rows[i] = new Row(i, values, labelValue);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return new Table(nRows, nDataCols, rows);
	}
	
	private static int getNumCols(String filename, String sep) {
		try {
			Stream<String> stream = Files.lines(Paths.get(filename));
			String header = stream.iterator().next();
			return (int ) StringUtils.countMatches(header, sep) + 1;
		} catch (IOException e) {
			return 0;
		}
	}
	
	private static int getNumRows(String filename) {
		try {
			Path path = Paths.get(filename);
			return (int) Files.lines(path).count();
		} catch (IOException e) {
			return 0;
		}
	}
	
	public List<Row> asList() {
		return Arrays.asList(this.rows);
	}
	
	@Override
	public Table clone() {
		Row[] rows = new Row[numRows];
		for (int i = 0; i < numRows; i++)
			rows[i] = new Row(this.rows[i].id, this.rows[i].values);
		return new Table(numRows, numCols, rows);
	}

	public void normalize() {
		double x, minVal, maxVal;

		for (int j = 0; j < numCols; j++) {
			minVal = Double.MAX_VALUE;
			maxVal = Double.MIN_VALUE;
			
			for (int i = 0; i < numRows; i++) {
				x = rows[i].values[j];
				minVal = Math.min(minVal, x);
				maxVal = Math.max(maxVal, x);
			}
			
			for (int i = 0; i < numRows; i++) {
				x = rows[i].values[j];
				rows[i].values[j] = (x - minVal) / (maxVal - minVal);
			}
		}
		
	}
}
