package dsi.kvmodel.microarray;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.math.DenseVector;

import jsc.correlation.KendallCorrelation;
import jsc.datastructures.PairedData;

public class TestJSCPerformance {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		double [][] a = new double [547][17814];
		BufferedReader filein = null;
		String line;
		try {
			filein = new BufferedReader(new FileReader("tcga.data"));
			int i = 0;
			while ((line = filein.readLine()) != null) {
				StringTokenizer st = new StringTokenizer(line, ",");
				int j = 0;
				while (st.hasMoreTokens()) {
					a[i][j++] = Double.parseDouble(st.nextToken());
				}	
				i++;
			}
		} catch (FileNotFoundException e) {
			System.out.println("cannot find the file.");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				filein.close();
			} catch (IOException e) {
				System.out.println("cannot close the file.");
				e.printStackTrace();
			}
		}
		long ts1 = System.currentTimeMillis();
		
		//SquaredEuclideanDistanceMeasure euDist = new SquaredEuclideanDistanceMeasure();
		//DecimalFormat f = new DecimalFormat("0.000000");
		
		//System.out.println(f.format(euDist.distance(new DenseVector(a[0]),new DenseVector(a[1]))));
		
		for(int i = 0; i < 547; i++) 
			for (int j = i + 1; j < 547; j++) {
				double sum = 0;
				for (int k = 0; k < 17814; k++)
				{
					sum += ((a[i][k]-a[j][k]) * (a[i][k]-a[j][k]));
					//new KendallCorrelation(new PairedData(a[i], a[1])).getR();
					//System.out.println(i);
					//f.format(euDist.distance(new DenseVector(a[j]),new DenseVector(a[i])));
					//System.out.println(a[1][i]);
				}
				Math.sqrt(sum);
				//System  .out.println(sum);
				//break;
			}
		long ts2 = System.currentTimeMillis();
		System.out.println("total time is " + (ts2 - ts1));
	}

}
