package dsi.kvmodel.snp;

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



public class MysqlConnSNP {
	
	
	
	public MysqlConnSNP() throws SQLException, ClassNotFoundException {				
			Class.forName("com.mysql.jdbc.Driver");
	}
	
	public static int insert(String ip, String filename) {
			
			BufferedReader br = null;
			String str = null;
			System.out.println(filename);
			long ts1 = System.currentTimeMillis();
			System.out.println("start time is " + ts1);
			Connection connection = null;
			//PreparedStatement psind = null;
			//PreparedStatement psrs = null;
			PreparedStatement psobs = null;
			int count = 0;
			try {				
				try {
					Class.forName("com.mysql.jdbc.Driver");
				} catch (ClassNotFoundException e) {
					System.out.println("Where is your MySQL JDBC Driver?");
					e.printStackTrace();
					return -1;
				}

				String sqlind = "INSERT INTO `snpdata`.`Individual` (`ind_id`, `pop_id`) VALUES (?, `ind_id`);";
				String sqlrs = "INSERT INTO `snpdata`.`SNP`(`snp_id`,`allele`,`chrom`,`pos`,`strand`,`assembly`,`center`,`protLSID`,`assayLSID`,`panelLSID`,`QCcode`)VALUES(?,?,?,?,?,?,?,?,?,?,?);";
				String sqlobs = "INSERT INTO `snpdata`.`Obs`(`gty_obs`,`ind_id`,`snp_id`)VALUES(?,?,?);"; 
				connection = DriverManager.getConnection("jdbc:mysql://" + ip + ":3306/snpdata?user=root&password=wsc.19840703&autoReconnect=true");
				//psind = connection.prepareStatement(sqlind);
				//psrs = connection.prepareStatement(sqlrs);
				psobs = connection.prepareStatement(sqlobs);
				
				List<String> subList = new ArrayList<String>();
				br = new BufferedReader(new FileReader(new File(filename)));
				while ((str = br.readLine()) != null) {
					StringTokenizer tokenizer = new StringTokenizer(str, " ");
					String rs = tokenizer.nextToken();
					String alleles = tokenizer.nextToken();
					String chrom = tokenizer.nextToken();
					String pos = tokenizer.nextToken();
					String strand = tokenizer.nextToken();
					String assembly = tokenizer.nextToken();
					String center = tokenizer.nextToken();
					String protLSID = tokenizer.nextToken();
					String assayLSID = tokenizer.nextToken();
					String panelLSID = tokenizer.nextToken();
					String QCcode = tokenizer.nextToken();
					if (rs.equals("rs#")) {					
						while (tokenizer.hasMoreTokens()) {
							String subject = tokenizer.nextToken();
							subList.add(subject);
							//psind.setString(1, subject);
							//psind.addBatch();
						}
						//psind.executeBatch();
						continue;
					} else {
						int i = 0;
						/*
						psrs.setString(1, rs);
						psrs.setString(2, alleles);
						psrs.setString(3, chrom);
						psrs.setString(4, pos);
						psrs.setString(5, strand);
						psrs.setString(6, assembly);
						psrs.setString(7, center);
						psrs.setString(8, protLSID);
						psrs.setString(9, assayLSID);
						psrs.setString(10, panelLSID);
						psrs.setString(11, QCcode);
						psrs.addBatch();
						psrs.executeBatch();*/
						while (tokenizer.hasMoreTokens()) {
							psobs.setString(1, tokenizer.nextToken());
							psobs.setString(2, subList.get(i++));
							psobs.setString(3, rs);
							psobs.addBatch();
						}
						count ++;
						if (count % 10 == 0) {
							psobs.executeBatch();
							System.out.println(count);
						}
					}
				}
				psobs.executeBatch();
				long ts2 = System.currentTimeMillis();
				System.out.println("finish time is " + (ts2 - ts1));
				
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				System.out.println("we got it a SQL failure and restarted from " + count);
				e.printStackTrace();
				return count;
			} finally {
				System.out.println("final count is " + count);
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				try {
					//psind.close();
					//psrs.close();
					psobs.close();
					connection.close();			
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			return count;
	}
	
	public static void scan (String ip, String chromStart, String chromEnd,
			String posStart, String posEnd, String indStart, String indEnd) {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("MySQL JDBC Driver not found");
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
					.getConnection("jdbc:mysql://" + ip + ":3306/snpdata?"
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

			ts = System.currentTimeMillis();
			//String sql ="SELECT * from `snpdata`.`Obs` where " + field + " >= '"
			//		+ start + "' and " + field + " <= '" + end + "'";
			String sql = "SELECT * FROM snpdata.Obs o, snpdata.SNP s where o.snp_id = s.snp_id and " + 
			 "s.chrom >= '" + chromStart + "' and s.chrom <= '" + chromEnd + "' and " + 
			 "s.pos >= " + posStart + " and s.pos <= " + posEnd + " and " +
			 "o.ind_id >= '" + indStart + "' and o.ind_id <= '" + indEnd + "'";
			// start = temp + 1;
			rs = stmt.executeQuery(sql);
			// rs.setFetchSize(1000);

			// System.out.println(rs.getRow());
			count = 0;
			while (rs.next()) {
				// rs.getString("trial_name");
				count++;
				if (count % 10 == 0) {
					System.out.println(count + " current time is "
							+ (System.currentTimeMillis() - ts));
				}
				//54.769s
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
	
	public static void randomread (String ip, String chromFile,
			String posFile, String indStart, String indEnd) {
		
		BufferedReader chromReader = null;
		BufferedReader posReader = null;
		//BufferedReader indReader = null;
		String str = null;
		String chromString = "";
		String posString = "";
		//String indString = "";		
		try {
			chromReader = new BufferedReader(new FileReader(new File(chromFile)));
			while ((str = chromReader.readLine()) != null) {
				chromString = chromString + "'" + str + "',";
			}
			chromString = chromString.substring(0, chromString.length() - 1);
			
			posReader = new BufferedReader(new FileReader(new File(posFile)));
			while ((str = posReader.readLine()) != null) {
				posString = posString + "'" + str + "',";
			}
			posString = posString.substring(0, posString.length() - 1);
			/*
			indReader = new BufferedReader(new FileReader(new File(indFile)));
			while ((str = indReader.readLine()) != null) {
				indString = indString + "'" + str + "',";
			}
			indString = indString.substring(0, indString.length() - 1);*/
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		} finally {
			try {
				chromReader.close();
				posReader.close();
				//indReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("MySQL JDBC Driver not found");
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
					.getConnection("jdbc:mysql://" + ip + ":3306/snpdata?"
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

			ts = System.currentTimeMillis();
			//String sql ="SELECT * from `snpdata`.`Obs` where " + field + " >= '"
			//		+ start + "' and " + field + " <= '" + end + "'";
			String sql = "SELECT * FROM snpdata.Obs o, snpdata.SNP s where o.snp_id = s.snp_id and " + 
			 "s.chrom in (" + chromString + ") and " + 
			 "s.pos in (" + posString + ") and  " +
			 "o.ind_id >= '" + indStart + "' and o.ind_id <= '" + indEnd + "';";
			 //"o.ind_id in (" + indString + ")";
			// start = temp + 1;
			rs = stmt.executeQuery(sql);
			// rs.setFetchSize(1000);

			System.out.println(sql);
			count = 0;
			while (rs.next()) {
				// rs.getString("trial_name");
				count++;
				if (count % 10000 == 0) {
					System.out.println(count + " current time is "
							+ (System.currentTimeMillis() - ts));
				}
				//54.769s
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
	
	
	public static void select(String ip, String field, String item) {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("cannot find a valid MySQL JDBC Driver");
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
					.getConnection("jdbc:mysql://" + ip + ":3306/snpdata?"
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
			System.out.println("item is " + item);
			
			ts = System.currentTimeMillis();
			String sql ="SELECT * from `snpdata`.`Obs` where " + field + " = '"
					+ item + "'";
			// start = temp + 1;
			rs = stmt.executeQuery(sql);
			// rs.setFetchSize(1000);

			// System.out.println(rs.getRow());
			count = 0;
			while (rs.next()) {
				// rs.getString("trial_name");
				count++;
				if (count % 1000000 == 0) {
					System.out.println(count + " current time is "
							+ (System.currentTimeMillis() - ts));
				}
				//54.769s
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
	
	private class MultipleReader implements Runnable {
		private String ip;
		private String chromFile;
		private String posFile;
		private String indStart;
		private String indEnd;
		public MultipleReader (String ip, String chromFile,
				String posFile, String indStart, String indEnd) {
			this.ip = ip;
			this.chromFile = chromFile;
			this.posFile = posFile;
			this.indStart = indStart;
			this.indEnd = indEnd;
		}
		public void run() {
			
			BufferedReader chromReader = null;
			BufferedReader posReader = null;
			//BufferedReader indReader = null;
			String str = null;
			String chromString = "";
			String posString = "";
			//String indString = "";		
			try {
				chromReader = new BufferedReader(new FileReader(new File(chromFile)));
				while ((str = chromReader.readLine()) != null) {
					chromString = chromString + "'" + str + "',";
				}
				chromString = chromString.substring(0, chromString.length() - 1);
				
				posReader = new BufferedReader(new FileReader(new File(posFile)));
				while ((str = posReader.readLine()) != null) {
					posString = posString + "'" + str + "',";
				}
				posString = posString.substring(0, posString.length() - 1);
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			} finally {
				try {
					chromReader.close();
					posReader.close();
					//indReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			try {
				Class.forName("com.mysql.jdbc.Driver");
			} catch (ClassNotFoundException e) {
				System.out.println("MySQL JDBC Driver not found");
				e.printStackTrace();
				return;
			}
			ResultSet rs = null;
			Statement stmt = null;
			Connection conn = null;
			
			// int max = 6;

			// for (int i = 1; i <= max; i++) {
			int count = 0;
			long ts = System.currentTimeMillis();
			try {
				conn = DriverManager.getConnection("jdbc:mysql://" + ip + ":3306/snpdata?user=root&password=wsc.19840703&autoReconnect=true");
				stmt = conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
						java.sql.ResultSet.CONCUR_READ_ONLY);
				// stmt =
				// conn.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE,
				// java.sql.ResultSet.CONCUR_UPDATABLE);
				stmt.setFetchSize(Integer.MIN_VALUE);
				// stmt.setMaxRows(10000);
				System.out.println("fetch size is " + stmt.getFetchSize());
				System.out.println("max row is " + stmt.getMaxRows());

				ts = System.currentTimeMillis();
				//String sql ="SELECT * from `snpdata`.`Obs` where " + field + " >= '"
				//		+ start + "' and " + field + " <= '" + end + "'";
				String sql = "SELECT * FROM snpdata.Obs o, snpdata.SNP s where o.snp_id = s.snp_id and " + 
				 "s.chrom in (" + chromString + ") and " + 
				 "s.pos in (" + posString + ") and  " +
				 "o.ind_id >= '" + indStart + "' and o.ind_id <= '" + indEnd + "';";
				 //"o.ind_id in (" + indString + ")";
				// start = temp + 1;
				rs = stmt.executeQuery(sql);
				// rs.setFetchSize(1000);

				System.out.println(sql);
				count = 0;
				while (rs.next()) {
					// rs.getString("trial_name");
					count++;
					if (count % 10000 == 0) {
						System.out.println(count + " current time is "
								+ (System.currentTimeMillis() - ts));
					}
					//54.769s
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
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}	
		}
	}
	
	public void scheduler (String ip, String chromFile, String posFile, String indStart, String indEnd, int threadNum) {
		for (int i = 0; i < threadNum; i++) {
			Runnable r = new MultipleReader(ip, chromFile, posFile, indStart, indEnd);
			Thread t = new Thread(r);
			t.start();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("start time: " + System.currentTimeMillis());
		if (args.length < 1) {
			System.out.println("please input an argument");
			System.out
					.println("scan for scan a table and you also need input the table name, start row name, stop row name, patient number");
			System.out.println("insert for insert data into the table, parameter table name");
			System.out.println("get for getting record");
			return;
		}

		if (args[0].equals("scan")) {
			//ip,  start, end
			scan(args[1], args[2], args[3], args[4], args[5], args[6], args[7]);
		} else if (args[0].equals("insert")) {
			insert(args[1], args[2]);
		} else if (args[0].equals("get")) {
			select(args[1], args[2], args[3]) ;
		} else if (args[0].equals("randomread")) {
			randomread(args[1], args[2], args[3], args[4], args[5]) ;
		} else if (args[0].equals("conrandomread")) {
			MysqlConnSNP mysqlconn = null;
			try {
				mysqlconn = new MysqlConnSNP();
				mysqlconn.scheduler(args[1], args[2], args[3], args[4], args[5], Integer.parseInt(args[6]));
				
			} catch (SQLException e) {
				System.out.println("cannot connect to mysql driver");
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				System.out.println("mysql driver not found");
				e.printStackTrace();
			} 
		} else {
			System.out.println("please input an argument");
			System.out
					.println("init for create a new table with a family info");
			System.out
					.println("scan for scan a table and you also need input the table name, start row name, stop row name, patient number");
			System.out.println("insert for insert data into the table, parameter table name");
			System.out.println("get for getting record, first line is ip, second line is field, and the following lines are items in this field");
			return;
		}
		System.out.println("end time: " + System.currentTimeMillis());
		//insertMySQL("xab");
	}

}
