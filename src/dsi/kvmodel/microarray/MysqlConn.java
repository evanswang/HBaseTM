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

public class MysqlConn {
	
	public static void insertMySQLbyMatrix () {
		insertMySQLbyMatrix("11.11.0.48", "GSE24080", "GSE24080.row.data", "gene24080.csv", "annoGPL570", "patid24080.csv", 5000, 0);
	}
	
	public static void insertMySQLbyMatrix (String ip, String studyname, 
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
			try {
				Class.forName("com.mysql.jdbc.Driver");
			} catch (ClassNotFoundException e) {
				System.out.println("Where is your MySQL JDBC Driver?");
				e.printStackTrace();
			}
			String sql = "insert into microarray.Microarray (gene_symbol, probeset_id, subject_id, trial_name, raw, log, zscore) values (?, ?, ?, ?, ?, ?, ?)";
			connection = DriverManager.getConnection("jdbc:mysql://" + ip + ":3306/microarray?user=root&password=wsc.19840703");
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
					ps.setString(1, geneList.get(geneId));					
					ps.setString(2, annoList.get(probeId));
					ps.setString(3, paList.get(patientId));
					ps.setString(4, studyname);
					ps.setString(5, raw);
					ps.setString(6, raw);
					//ps.setString(8, mean);
					//ps.setString(9, median);
					ps.setString(7, raw);
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
	
	public static int insertMySQL(String filename, int location) {
			BufferedReader br = null;
			String str = null;
			System.out.println(filename);
			long ts1 = System.currentTimeMillis();
			System.out.println("start time is " + ts1);
			Connection connection = null;
			PreparedStatement ps = null;
			int count = 0;
			try {				
				try {
					Class.forName("com.mysql.jdbc.Driver");
				} catch (ClassNotFoundException e) {
					System.out.println("Where is your MySQL JDBC Driver?");
					e.printStackTrace();
					return -1;
				}
				final int batchSize = 200;
				count = 0;
				String sql = "insert into Data.MicroarrayDisk (gene_symbol, probeset_id, patient_id, subject_id, trial_name, raw, log, mean, median, zscore) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
				connection = DriverManager.getConnection("jdbc:mysql://146.169.32.135:3306/microarray?user=root&password=wsc.19840703");
				ps = connection.prepareStatement(sql);
				
				br = new BufferedReader(new FileReader(new File("/disk2/microarray/"
						+ filename)));
				while ((str = br.readLine()) != null) {
					str = str.replaceAll(" ", "");
					str = str.replaceAll(",,", ", ,");
					StringTokenizer tokenizer = new StringTokenizer(str, ",");
					String trialName = tokenizer.nextToken();
					if (trialName.equals("TRIAL_NAME"))
						continue;
					++count;
					if (count < location)
						continue;
					String gene = tokenizer.nextToken();
					String patientID = tokenizer.nextToken();
					String probeset = tokenizer.nextToken();
					String pvalue = tokenizer.nextToken();
					String subjectID = tokenizer.nextToken();
					String raw = tokenizer.nextToken();
					String log = tokenizer.nextToken();
					String mean = tokenizer.nextToken();
					String stddev = tokenizer.nextToken();
					String median = tokenizer.nextToken();
					String zscore = tokenizer.nextToken();
					
					ps.setString(1, gene);					
					ps.setString(2, probeset);
					ps.setString(3, patientID);
					ps.setString(4, subjectID);
					ps.setString(5, trialName);
					ps.setString(6, raw);
					ps.setString(7, log);
					//ps.setString(8, mean);
					//ps.setString(9, median);
					ps.setString(10, zscore);
					ps.addBatch();
					//ps.executeBatch();
					//ps.executeUpdate();
					if(count % batchSize == 0) {
						ps.executeBatch();
						//break;
						if (count % 5000 == 0) 
							System.out.println(count);
						try {
							Thread.sleep(500);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				//	break;
				}
				ps.executeBatch();
				long ts2 = System.currentTimeMillis();
				System.out.println("finish time is " + (ts2 - ts1));
				
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				System.out.println("we got it a SQL failure and restarted from " + count);
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				return count;
			} finally {
				System.out.println("final count is " + count);
				try {
					br.close();
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
			return count;
	}
	
	public static void scan (String start, String end) {
		scan("11.11.0.52", start, end);
	}
	
	public static void scan (String ip, String start, String end) {


		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Where is your MySQL JDBC Driver?");
			e.printStackTrace();
			return;
		}

		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;

		// int max = 6;

		// for (int i = 1; i <= max; i++) {
		int count = 0;
		long ts = System.currentTimeMillis();
		try {
			conn = DriverManager
					.getConnection("jdbc:mysql://" + ip + ":3306/microarray?"
							+ "user=root&password=wsc.19840703&autoReconnect=true");
			stmt = conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
					java.sql.ResultSet.CONCUR_READ_ONLY);
			// stmt =
			// conn.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE,
			// java.sql.ResultSet.CONCUR_UPDATABLE);
			stmt.setFetchSize(Integer.MIN_VALUE);
			// stmt.setMaxRows(10000);
			System.out.println("fetch size is " + stmt.getFetchSize());
			System.out.println("max row is " + stmt.getMaxRows());
			System.out.println("start " + start);
			System.out.println("end " + end);
			ts = System.currentTimeMillis();
			String sql ="SELECT * from `microarray`.`Microarray` where trial_name = 'GSE24080' and probeset_id >= "
					+ start + " and probeset_id <= " + end;
			// start = temp + 1;
			rs = stmt.executeQuery(sql);
			// rs.setFetchSize(1000);

			// System.out.println(rs.getRow());
			count = 0;
			while (rs.next()) {
				// rs.getString("trial_name");
				count++;
				if (count % 100000 == 0) {
					System.out.println(count + " current time is "
							+ (System.currentTimeMillis() - ts));
				}
				// if (count >= 2400840)
				// break;
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			System.out.println("total number is " + count);
			System.out.println("total time is "
					+ (System.currentTimeMillis() - ts));
			try {
				rs.close();
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		// }

	
	}
	
	public static void selectSubject(String filename) {

		BufferedReader br = null;
		String readStr = null;
		String sqlStr = "";
		//ArrayList<String> patientList = new ArrayList<String>();
		try {
			br = new BufferedReader(new FileReader(new File(filename)));
			while ((readStr = br.readLine()) != null) {
				sqlStr += readStr;
			}
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Where is your MySQL JDBC Driver?");
			e.printStackTrace();
			return;
		}

		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		int start = 79109;
		int end = 79667;

		// int max = 6;

		// for (int i = 1; i <= max; i++) {
		int count = 0;
		long ts = System.currentTimeMillis();
		try {
			conn = DriverManager
					.getConnection("jdbc:mysql://146.169.35.147:3306/Data?"
							+ "user=root&password=19840703&autoReconnect=true");
			stmt = conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
					java.sql.ResultSet.CONCUR_READ_ONLY);
			// stmt =
			// conn.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE,
			// java.sql.ResultSet.CONCUR_UPDATABLE);
			stmt.setFetchSize(Integer.MIN_VALUE);
			// stmt.setMaxRows(10000);
			System.out.println("fetch size is " + stmt.getFetchSize());
			System.out.println("max row is " + stmt.getMaxRows());
			System.out.println("start " + start);
			System.out.println("end " + end);
			ts = System.currentTimeMillis();
			String sql = "SELECT * from `Data`.`MicroarrayDisk` where trial_name = 'MULTMYEL' and patient_id in (" + sqlStr + ")";
			//String sql ="SELECT * from `Data`.`MicroarrayDisk` where trial_name = 'MULTMYEL' and patient_id >= "
			//		+ start + " and patient_id <= " + end;
			// start = temp + 1;
			rs = stmt.executeQuery(sql);
			// rs.setFetchSize(1000);

			// System.out.println(rs.getRow());
			count = 0;
			while (rs.next()) {
				// rs.getString("trial_name");
				count++;
				if (count % 100000 == 0) {
					System.out.println(count + " current time is "
							+ (System.currentTimeMillis() - ts));
				}
				// if (count >= 2400840)
				// break;
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			System.out.println("total number is " + count);
			System.out.println("total time is "
					+ (System.currentTimeMillis() - ts));
			try {
				rs.close();
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		// }

	}
	
	public static void selectProbe(String ip, String trial, String probeFile) {

		BufferedReader br = null;
		String readStr = null;
		String sqlStr = "";
		//ArrayList<String> patientList = new ArrayList<String>();
		try {
			br = new BufferedReader(new FileReader(new File(probeFile)));
			while ((readStr = br.readLine()) != null) {
				sqlStr += "'" + readStr + "',";
			}
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		sqlStr = sqlStr.substring(0, sqlStr.length() - 1);

		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Where is your MySQL JDBC Driver?");
			e.printStackTrace();
			return;
		}

		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;

		// int max = 6;

		// for (int i = 1; i <= max; i++) {
		int count = 0;
		try {
			conn = DriverManager
					.getConnection("jdbc:mysql://" + ip + ":3306/microarray?"
							+ "user=root&password=wsc.19840703&autoReconnect=true");
			stmt = conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
					java.sql.ResultSet.CONCUR_READ_ONLY);
			stmt.setFetchSize(Integer.MIN_VALUE);
			System.out.println("fetch size is " + stmt.getFetchSize());
			System.out.println("max row is " + stmt.getMaxRows());
			String sql = "SELECT * from `microarray`.`Microarray` where trial_name = '" 
					+ trial + "' and probeset_id in (" + sqlStr + ")";
			long ts1 = System.currentTimeMillis();
			rs = stmt.executeQuery(sql);
			count = 0;
			while (rs.next()) {
				// rs.getString("trial_name");
				count++;
			}
			long ts2 = System.currentTimeMillis();
			System.out.println("count:" + count 
					+ ", time:" + (ts2 - ts1) + ", current time:" + ts2);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				rs.close();
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		// }
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
			insertMySQLbyMatrix(ip, studyname, datafile, genefilename,
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
	
	private class MultipleReader implements Runnable {
		String ip;
		String trial;
		String probeFile;
		public MultipleReader (String ip, String trial, String probeFile) {
			this.ip = ip;
			this.trial = trial;
			this.probeFile = probeFile;
		}
		public void run() {
			selectProbe(ip, trial, probeFile);
		}
	}
	
	public void schedulerReader (String trial, String probeFile, int threadNum) {
		for (int i = 0; i < threadNum; i++) {
			Runnable r = new MultipleReader("mysql01", trial, probeFile);
			Thread t = new Thread(r);
			t.start();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		if (args.length < 1) {
			System.out.println("please input an argument");
			System.out
					.println("scan for scan a table and you also need input the table name, start row name, stop row name, patient number");
			System.out.println("insert for insert data into the table, parameter table name");
			System.out.println("get for getting record");
			return;
		}

		if (args[0].equals("scan")) {
			scan(args[1], args[2]);
		} else if (args[0].equals("insert")) {
			String startStr = args[1];
			String endStr = args[2];
			int location = Integer.parseInt(args[3]);
			int curLocate = 0;
			int fileLength = 925000;
			boolean isFirstFile = true;
			boolean isRestarted = false;
			char startCh1 = startStr.charAt(1);
			char startCh2 = startStr.charAt(2);
			char endCh1 = endStr.charAt(1);
			char endCh2 = endStr.charAt(2);
			for (int i = startCh1; i <= endCh1; i++)
				for (int j = 'a'; j <= 'z'; j++) {
					if (i == startCh1 && j < startCh2) {
						// for the first file, it starts from the third char
						// of the file name
							continue;
					}
					if (i == endCh1 && j > endCh2) {
						// end file
							break;
					}
					System.out.println("x" + (char)i + (char)j);
					int count = 0;
					if (isFirstFile) {
						// the first file starts from the specific location
						count = location;
						isFirstFile = false;
					}
					if (isRestarted) {
						// after a SQL failure, restart from last update location
						count = curLocate - 200;
						isRestarted = false;
					}				
					count = insertMySQL("x" + (char)i + (char)j, count);
					if (count < fileLength) {
						// if SQL failed, re-insert the file from the last update
						// location, and keep the location in curLocate
						j--;
						isRestarted = true;
						curLocate = count;
					}
				}
			// System.out.println("x" + (char)('a' + i) + (char)('a' + j));

		} else if (args[0].equals("get")) {
			selectSubject(args[1]) ;
		} else if (args[0].equals("get-probe")) {
			MysqlConn mysql = new MysqlConn();
			mysql.schedulerReader(args[1], args[2], Integer.parseInt(args[3]));
		} else if (args[0].equals("insertbymatrixdefault")) {
			insertMySQLbyMatrix();
		} else if (args[0].equals("insertbymatrix")) {
			insertMySQLbyMatrix(args[1], args[2], args[3], args[4], args[5], args[6], Long.parseLong(args[7]), Long.parseLong(args[8]));
		} else if (args[0].equals("multi-insert")) {
			MysqlConn mysql = new MysqlConn();
			mysql.schedulerInsert(args[1], args[2], args[3], args[4], 100, Long.parseLong(args[5]));
		} else {
			System.out.println("please input an argument");
			System.out
					.println("init for create a new table with a family info");
			System.out
					.println("scan for scan a table and you also need input the table name, start row name, stop row name, patient number");
			System.out.println("insert for insert data into the table, parameter table name");
			System.out.println("get for getting record");
			return;
		}
		 
		//insertMySQL("xab");
	}

}
