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
				if (psnum != 54675)
					System.out.println("Incomplete probe set: " + Bytes.toString(rr.getRow()) + " " + psnum);
				
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
	
	public void getRecord(String filename) {
		getRecord(filename, 1);
	}

	public void getRecord(String filename, int cache) {
		ArrayList<String> patientList = new ArrayList<String>();
		BufferedReader reader = null;
		String line = null;
		try {
			reader = new BufferedReader(new FileReader(filename));
			while ((line = reader.readLine()) != null) {
				patientList.add(line);
			}
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		long count = 0;
		long ts = System.currentTimeMillis();
		List<Get> getList = new ArrayList<Get>();
		try {
			for (String str : patientList) {
				Get get = new Get(Bytes.toBytes(str));
				get.addFamily(Bytes.toBytes(COL_FAMILY_RAW));
				//g.setFilter(new ColumnPrefixFilter("A".getBytes()));
				getList.add(get);
				
				if (getList.size() == cache) {
					int psnum = 0;
					Result[] rr = MicroarrayTable.get(getList);
					for (Result r : rr) {
						for (Cell cell : r.rawCells()) {
							psnum++;
						}
						if (psnum != 54675)
							System.out
									.println("getRecord incomplete probe set: "
											+ Bytes.toString(r.getRow()) + " "
											+ psnum);
						System.out.println("getRecord result " + count++);
						if (r.isEmpty())
							System.out.println("no result");
					}
					getList.clear();
				}
			}
			
			int psnum = 0;
			Result[] rr = MicroarrayTable.get(getList);
			for (Result r : rr) {
				for (Cell cell : r.rawCells()) {
					psnum++;
				}
				if (psnum != 54675)
					System.out
							.println("getRecord incomplete probe set: "
									+ Bytes.toString(r.getRow()) + " "
									+ psnum);
				System.out.println("getRecord result " + count++);
				if (r.isEmpty())
					System.out.println("no result");
			}
			getList.clear();
			
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
			HBasePerfTest.init(args[1]);
		} else if (args[0].equals("scan")) {
			HBasePerfTest hbasetm = new HBasePerfTest(args[1]);
			hbasetm.tableScan(args[2], args[3], Integer.parseInt(args[4]), Integer.parseInt(args[5]));
		} else if (args[0].equals("get")) {
			HBasePerfTest hbasetm = new HBasePerfTest(args[1]);
			hbasetm.getRecord(args[2]);
		} else if (args[0].equals("getlist")) {
			HBasePerfTest hbasetm = new HBasePerfTest(args[1]);
			hbasetm.getRecord(args[2], Integer.parseInt(args[3]));
		} else {
			System.out.println("please input an argument");
			System.out.println("init for create a new table with a family info");
			System.out.println("scan for scan a table and you also need input the table name, start row name, stop row name, maximum patient number");
			System.out.println("insertMatrixBySubject for insert data into the table, parameter table name, data file, annotation file, patient file, cache size");
			System.out.println("get for getting record");
			return;
		}		
	}
}

