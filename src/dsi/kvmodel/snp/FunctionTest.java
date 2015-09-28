package dsi.kvmodel.snp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class FunctionTest {
	public static void testLoad () {
		
	}
	
	public static void insertSubIndSNP(String file) {

		BufferedReader br = null;
		String str = null;
		System.out.println("subject is " + file);
		long ts1 = System.currentTimeMillis();
		System.out.println("start inserting SubInd table at " + ts1);

		int count = 0;
		try {				
			br = new BufferedReader(new FileReader(new File(file)));
			while ((str = br.readLine()) != null) {
				StringTokenizer tokenizer = new StringTokenizer(str, "\t");
				String PATIENTID= tokenizer.nextToken(); // not id, but a name. not used
				String SAMPLETYPE= tokenizer.nextToken();
				String TIMEPOINT= tokenizer.nextToken();
				String TISSUETYPE= tokenizer.nextToken();
				String GPLID= tokenizer.nextToken();
				String ASSAYID= tokenizer.nextToken();
				String SAMPLECODE= tokenizer.nextToken();
				String REFERENCE= tokenizer.nextToken();
				String VARIANT= tokenizer.nextToken();
				String VARIANTTYPE= tokenizer.nextToken();
				String CHROMOSOME= tokenizer.nextToken(); // row key
				String POSITION= tokenizer.nextToken(); // row key
				String RSID= tokenizer.nextToken();
				String REFERENCEALLELE= tokenizer.nextToken();
				count ++;
				
				
			}
			
			System.out.println("final count is " + count);
			long ts2 = System.currentTimeMillis();
			System.out.println("finish time is " + (ts2 - ts1));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}	
		}
	}
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		FunctionTest.insertSubIndSNP("/Users/EvanSWang/Downloads/admin-DataExport-110663/subset1_CELL-LINE/1000384994");
	}

}
