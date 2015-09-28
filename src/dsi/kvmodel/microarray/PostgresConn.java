package dsi.kvmodel.microarray;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class PostgresConn {

	
	public static void insertPSQLbyMatrix () {
		insertPSQLbyMatrix("localhost", "GSE24080", "../data/GSE24080.row.data", "../data/GSE24080.probe", "../data/GSE24080.probe", "../data/GSE24080.patient", 5000, 0);
	}
	
	public static void insertPSQLbyMatrix (String ip, String studyname, 
			String datafile, String genefilename, String probefilename, 
			String patientfilename, long cachesize, long starter) {
		BufferedReader filein = null;
		BufferedReader annoIn = null;
		BufferedReader geneIn = null;
		BufferedReader paIn = null;
		String line;
		StringTokenizer stin; // for deep token parse
		int count = 0;
		long ts1 = System.currentTimeMillis();
		System.out.println("start time is " + ts1);
		Connection connection = null;
		PreparedStatement ps = null;
		try {
			String sql = "insert into deapp.de_subject_microarray_data (assay_id, probeset_id, subject_id, trial_name, raw_intensity, log_intensity, zscore) values (?, ?, ?, ?, ?, ?, ?)";
			connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/transmart?user=postgres&password=postgres&autoReconnect=true");
			ps = connection.prepareStatement(sql);
			filein = new BufferedReader(new FileReader(datafile));
			geneIn = new BufferedReader(new FileReader(genefilename));
			annoIn = new BufferedReader(new FileReader(probefilename));
			paIn = new BufferedReader(new FileReader(patientfilename));
			List<String> geneList = new ArrayList<String>();
			List<String> annoList = new ArrayList<String>();
			List<String> paList = new ArrayList<String>();
			while ((line = geneIn.readLine()) != null) {
				geneList.add(line);
			}
			while ((line = annoIn.readLine()) != null) {
				annoList.add(line);
			}
			while ((line = paIn.readLine()) != null) {
				paList.add(line);
			}
			System.out.println("file " + datafile);
			System.out.println("probe list length is " + annoList.size());
			int patientId = 0;
			while ((line = filein.readLine()) != null) {
				stin = new StringTokenizer(line, ",");
				int geneId = 0;
				int probeId = 0;
				if (patientId == paList.size())
					break;
				while(stin.hasMoreTokens()) {
					String raw = stin.nextToken();
					ps.setInt(1, Integer.parseInt(geneList.get(geneId)));					
					ps.setInt(2, Integer.parseInt(annoList.get(probeId)));
					ps.setString(3, paList.get(patientId));
					ps.setString(4, studyname);
					ps.setDouble(5, Double.parseDouble(raw));
					ps.setDouble(6, Double.parseDouble(raw));
					//ps.setString(8, mean);
					//ps.setString(9, median);
					ps.setDouble(7, Double.parseDouble(raw));
					if (count >= starter)
						ps.addBatch();
					geneId++; 
					probeId++;
					count++;
					if(count % cachesize == 0) {
						ps.executeBatch();
						if (count % 1000 == 0	)
							System.out.println(patientfilename + ":" + patientId + ":" + count);
					}
				}			
				patientId ++;
			}
			ps.executeBatch();
			System.out.println(patientfilename + ":" + patientId + ":final count is " + count);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e1) {
			System.out.println("we got it a SQL failure and restarted from " + count);
			e1.printStackTrace();
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e2) {
				e2.printStackTrace();
			}
			e1.printStackTrace();
		} finally {
			System.out.println("final count is " + count);
			try {
				filein.close();
				geneIn.close();
				annoIn.close();
				paIn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}			
			try {
				ps.close();
				connection.close();			
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	

	private class MultipleInsert implements Runnable {
		String ip;
		String studyname;
		String datafile;
		String genefilename;
		String probefilename;
		String patientfilename;
		long cachesize;
		long starter;

		public MultipleInsert(String ip, String studyname, String datafile,
				String genefilename, String probefilename,
				String patientfilename, long cachesize, long starter) {
			this.ip = ip;
			this.studyname = studyname;
			this.datafile = datafile;
			this.genefilename = genefilename;
			this.probefilename = probefilename;
			this.patientfilename = patientfilename;
			this.cachesize = cachesize;
			this.starter = starter;
		}

		public void run() {
			System.out.println(patientfilename + " start !!!!!!");
			insertPSQLbyMatrix(ip, studyname, datafile, genefilename,
					probefilename, patientfilename, cachesize, starter);
		}
	}
	
	public void schedulerInsert(String studyname, String datafile,
			String genefilename, String probefilename, long cachesize, long starter) {
		for (int i = 0; i < 40; i++) {
			if (i < 10) {
				Runnable r = new MultipleInsert("mysql01", studyname, datafile,
						genefilename, probefilename,
						"/data/testcode24080/data/x0" + i, cachesize, starter);
				Thread t = new Thread(r);
				t.start();
			} else if (i < 20) {
				Runnable r = new MultipleInsert("mysql02", studyname, datafile,
						genefilename, probefilename,
						"/data/testcode24080/data/x" + i, cachesize, starter);
				Thread t = new Thread(r);
				t.start();
			} else if (i < 30) {
				Runnable r = new MultipleInsert("mysql03", studyname, datafile,
						genefilename, probefilename,
						"/data/testcode24080/data/x" + i, cachesize, starter);
				Thread t = new Thread(r);
				t.start();
			} else {
				Runnable r = new MultipleInsert("mysql04", studyname, datafile,
						genefilename, probefilename,
						"/data/testcode24080/data/x" + i, cachesize, starter);
				Thread t = new Thread(r);
				t.start();
			}

		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		if (args.length < 1) {
			System.out.println("please input an argument");
			return;
		}

		if (args[0].equals("insertbymatrixdefault")) {
			insertPSQLbyMatrix();
		} else if (args[0].equals("insertbymatrix")) {
			insertPSQLbyMatrix(args[1], args[2], args[3], args[4], args[5], args[6], Long.parseLong(args[7]), Long.parseLong(args[8]));
		} else if (args[0].equals("multi-insert")) {
			PostgresConn mysql = new PostgresConn();
			mysql.schedulerInsert(args[1], args[2], args[3], args[4], 100, Long.parseLong(args[5]));
		} else {
			System.out.println("please input an argument");
			return;
		}
	}


}
