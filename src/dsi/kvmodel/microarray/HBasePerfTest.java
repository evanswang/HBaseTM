package dsi.kvmodel.microarray;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;



public class HBasePerfTest {


//	private static final String COL_FAMILY_INFO = "info";
	
	private static final String COL_FAMILY_RAW = "raw";
	private static final String COL_FAMILY_LOG = "log";
	private static final String COL_FAMILY_MEAN = "mean";
	private static final String COL_FAMILY_MEDIAN = "median";
	private static final String COL_FAMILY_ZSCORE = "zscore";
	
//	private static final String COL_FAMILY_PVALUE = "pval";
	private static final String PATIENT_ID = "patient_id";
	private static final String RAW_VALUE = "raw";
	private static final String LOG_VALUE = "log";
	private static final String MEAN_VALUE = "mean";
//	private static final String STDDEV_VALUE = "stddev";
	private static final String MEDIAN_VALUE = "median";
	private static final String ZSCORE = "z_score";
//	private static final String P_VALUE = "p_value";
//	private static final String GENE_SYMBOL = "gene_symbol";
//	private static final String PROBESET_ID = "probeset";

	Configuration config;
	HBaseAdmin hadmin;
	HTable MicroarrayTable;

	public HBasePerfTest(String table) {
		config = HBaseConfiguration.create();
		try {
			MicroarrayTable = new HTable(config, table);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void init(String tablename) {
		// create the microarray table
		try {
			Configuration config = HBaseConfiguration.create();
			HBaseAdmin hadmin = new HBaseAdmin(config);
			HTableDescriptor microarrayTableDesc = new HTableDescriptor(
					tablename);
	//		HColumnDescriptor infoColDesc = new HColumnDescriptor(COL_FAMILY_INFO);	
			HColumnDescriptor rawColDesc = new HColumnDescriptor(COL_FAMILY_RAW); 
			HColumnDescriptor logColDesc = new HColumnDescriptor(COL_FAMILY_LOG);
			HColumnDescriptor meanColDesc = new HColumnDescriptor(COL_FAMILY_MEAN);
			HColumnDescriptor medianColDesc = new HColumnDescriptor(COL_FAMILY_MEDIAN);
			HColumnDescriptor zscoreColDesc = new HColumnDescriptor(COL_FAMILY_ZSCORE);
			
	//		microarrayTableDesc.addFamily(infoColDesc);
					
			microarrayTableDesc.addFamily(rawColDesc);
			microarrayTableDesc.addFamily(logColDesc);
			microarrayTableDesc.addFamily(meanColDesc);
			microarrayTableDesc.addFamily(medianColDesc);
			microarrayTableDesc.addFamily(zscoreColDesc);
					
			hadmin.createTable(microarrayTableDesc);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void tableScan(String startrow, String stoprow, int threshold) {
		tableScan(startrow, stoprow, threshold, 1000);
	}

	public void tableScan(String startrow, String stoprow, int threshold,
			int cacheSize) {// only add family
		
		Scan s = new Scan();
		s.addFamily(Bytes.toBytes(COL_FAMILY_RAW));
		
		s.setCacheBlocks(true);
		s.setCaching(cacheSize);
		s.setStartRow(Bytes.toBytes(startrow));
		s.setStopRow(Bytes.toBytes(stoprow));
		// s.addColumn(Bytes.toBytes(COL_FAMILY_INFO),
		// 		Bytes.toBytes(PATIENT_ID));
		ResultScanner scanner = null;

		
		long count = 0;
		try {
			scanner = MicroarrayTable.getScanner(s);
			long ts1 = System.currentTimeMillis();
			for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
				// System.out.println("Found row: " + rr);
				if (count == threshold)
					break;
				int psnum = 0;
				for (Cell kv : rr.rawCells()) {
					psnum++;
					// each kv represents a column
					// System.out.println(Bytes.toString(kv.getRowArray()));
					// System.out.println(Bytes.toString(CellUtil.cloneQualifier(kv)));
					// System.out.println(Bytes.toString(CellUtil.cloneValue(kv)));
				}

				

				count++;
				if (count % 5000 == 0)
					System.out.println(count);
			}
			System.out.println("time is " + (System.currentTimeMillis() - ts1));
			System.out.println("total amount is " + count);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			scanner.close();
		}

	}

	public void getRecord(String filename, String prefix) {
		ArrayList<String> patientList = new ArrayList<String>();
		// TODO: add subjects to paList

		long count = 0;
		long ts = System.currentTimeMillis();
		//List<Get> getlist = new ArrayList<Get>();
		try {
			for (String str : patientList) {
				Get g = new Get(Bytes.toBytes(str));
				g.addFamily(Bytes.toBytes(COL_FAMILY_RAW));
				//g.setFilter(new ColumnPrefixFilter("A".getBytes()));
				// getlist.add(g);
				int psnum = 0;
				MicroarrayTable.setScannerCaching(10);
				Result r = MicroarrayTable.get(g);
				for (Cell cell : r.rawCells()) {
					psnum++;
				}
				if (psnum != 54675)
					System.out.println(Bytes.toString(r.getRow()) + " " + psnum);
				System.out.println("result " + count++);
				if (r.isEmpty())
					System.out.println("no result");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("get time is " + (System.currentTimeMillis() - ts));
	}
		
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		if (args.length < 1) {
			System.out.println("please input an argument");
			System.out
					.println("init for create a new table with a family info");
			System.out
					.println("scan for scan a table and you also need input the table name, start row name, stop row name, patient number");
			System.out.println("insert for insert data into the table, parameter table name");
			System.out.println("get for getting record");
			return;
		}
		if (args[0].equals("init")) {
			HBaseTM.init(args[1]);
		} else if (args[0].equals("scanBySubject")) {
			HBaseTM hbasetm = new HBaseTM(args[1]);
			hbasetm.tableScan(args[2], args[3], Integer.parseInt(args[4]));
		} else if (args[0].equals("scanByProbe")) {
			HBaseTM hbasetm = new HBaseTM(args[1]);
			hbasetm.tableScan(args[2], args[3], Integer.parseInt(args[4]), Integer.parseInt(args[5]), args[6]);
		} else if (args[0].equals("insert")) {
			/*
			 * parameters
			 * @tablename, which is also trial name
			 * @file path, which lists teh absolute path of all csv files
			 */
			HBaseTM hbasetm = new HBaseTM(args[1]);
			BufferedReader filein = null;
			String line;
			try {
				filein = new BufferedReader(new FileReader(args[2]));
				while ((line = filein.readLine()) != null) {
					hbasetm.insertMicroarray(args[1], line);
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
			//hbasetm.insertMicroarray("GSE4382.csv");
			/*
			for (int i = 0; i < 5; i++)
				for (int j = 0; j < 26; j++)
					hbasetm.insertMicroarray("x" + (char) ('a' + i)
							+ (char) ('a' + j));
							*/
			// System.out.println("x" + (char)('a' + i) + (char)('a' + j));

		} else if (args[0].equals("get")) {
			HBaseTM hbasetm = new HBaseTM(args[1]);
			hbasetm.getRecord(args[2], args[3], args[4]);
		} else if (args[0].equals("randomReadProbe")) {
			HBaseTM hbasetm = new HBaseTM(args[1]);
			hbasetm.randomReadProbe(args[2], args[3], Integer.parseInt(args[4]));
		} else if (args[0].equals("conrandomread")) {
			HBaseTM hbasetm = new HBaseTM(args[1]);
			hbasetm.scheduler(args[2], args[3], Integer.parseInt(args[4]));
		} else if (args[0].equals("concross")) {
			HBaseTM hbasetm = new HBaseTM(args[1]);
			hbasetm.schedulerCross(args[2], Integer.parseInt(args[3]));
		} else if (args[0].equals("concrossscan")) {
			HBaseTM hbasetm = new HBaseTM(args[1]);
			hbasetm.schedulerScanCross(args[2], Integer.parseInt(args[3]));
		} else if (args[0].equals("insertMatrixBySubject")) {
			HBaseTM hbasetm = new HBaseTM(args[1]);
			hbasetm.insert4MatrixBySubject(args[2], args[3], args[4], args[5], args[6], Long.parseLong(args[7]));
		} else if (args[0].equals("insertMatrixByProbe")) {
			HBaseTM hbasetm = new HBaseTM(args[1]);
			hbasetm.insert4MatrixByProbe(args[2], args[3], args[4], args[5], Long.parseLong(args[6]));
		} else if (args[0].equals("insertMatrixCross")) {
			HBaseTM hbasetm = new HBaseTM(args[1]);
			hbasetm.insert4MatrixCross(args[2], args[3], args[4], args[5], Long.parseLong(args[6]));
		} else {
			System.out.println("please input an argument");
			System.out.println("init for create a new table with a family info");
			System.out.println("scanBySubject for scan a table and you also need input the table name, start row name, stop row name, maximum patient number");
			System.out.println("scanByProbe for scan a table and you also need input the table name, start row name, stop row name, maximum probe number, cache size, patient list file");
			System.out.println("insert for insert data into the table, parameter table name");
			System.out.println("insertMatrixBySubject for insert data into the table, parameter table name, data file, annotation file, patient file, cache size");
			System.out.println("insertMatrixByProbe for insert data into the table, parameter table name, data file, annotation file, patient file, cache size");
			System.out.println("get for getting record");
			return;
		}		
	}
}
