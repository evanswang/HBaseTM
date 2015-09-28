package dsi.kvmodel.microarray;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class OracleConn {
	
	public void connectToAndQueryDatabase(String username, String password) {

	    try {
			Connection con = DriverManager.getConnection(
			                     "jdbc:oracle:thin:@//146.169.35.12:1521/orcl",
			                     "deapp",
			                     "deapp");

			Statement stmt = con.createStatement();
			long ts = System.currentTimeMillis();
			ResultSet rs = stmt.executeQuery("SELECT * FROM deapp.de_subject_microarray_data");

			while (rs.next()) {
			    
			}
			System.out.println("time is " + (System.currentTimeMillis() - ts));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		if (args.length < 2) {
			System.out.println("please input study name and patient num file path");
			return;
		}
		String line = null;
		String patientNums = "";
		BufferedReader patientFile = null;
		String studyName = args[0];	
		try {
			patientFile = new BufferedReader(new FileReader(args[1]));
			while ((line = patientFile.readLine()) != null) {
				patientNums = patientNums + line + ", ";
			}
			patientNums = patientNums.substring(0, patientNums.length()-2);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		} finally {
			try {
				patientFile.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	
		Connection conn = null;
		Statement stmt = null;
		
		try {
			conn = DriverManager.getConnection(
			                     "jdbc:oracle:thin:@//146.169.35.12:1521/orcl",
			                     "deapp",
			                     "deapp");
			
			
			stmt = conn.createStatement();
			long ts = System.currentTimeMillis();
			String sql = "SELECT GENE_SYMBOL, PATIENT_ID, PROBESET, SUBJECT_ID, RAW_INTENSITY FROM DEAPP.DE_SUBJECT_MICROARRAY_DATA WHERE TRIAL_NAME = '" + studyName + "' AND PATIENT_ID IN ( " + patientNums + " )";
			//ResultSet rs = stmt.executeQuery("SELECT GENE_SYMBOL, PATIENT_ID, PROBESET, SUBJECT_ID, RAW_INTENSITY FROM DEAPP.DE_SUBJECT_MICROARRAY_DATA WHERE TRIAL_NAME = 'eclipse2' AND PATIENT_ID IN (1004001127,	1004001307,	1004001919,	1004002340,	1004003269,	1004003542,	1004003701,	1004003848,	1004004428,	1004004925,	1004006558,	1004010017,	1004012285,	1004012886,	1004012897,	1004014826,	1004015143,	1004015744,	1004017378,	1004021607,	1004023399,	1004024907,	1004025224,	1004025973,	1004026098,	1004026383,	1004030317,	1004030442,	1004032743,	1004038437,	1004039660,	1004040240,	1004041420,	1004041442,	1004041578,	1004042518,	1004044152,	1004047758,	1004048813,	1004050310,	1004051217,	1004053157,	1004054529,	1004055856,	1004058758,	1004060255,	1004060845,	1004060993,	1004062217,	1004065675,	1004067774,	1004067910,	1004071391,	1004072888,	1004075451,	1004077527,	1004080669,	1004082789,	1004082948,	1004083412,	1004083423,	1004083549,	1004084445,	1004088062,	1004089128,	1004090466,	1004090488,	1004092270,	1004094832,	1004096783,	1004096931,	1004098291,	1004098881,	1004099357)");
			//ResultSet rs = stmt.executeQuery("SELECT GENE_SYMBOL, PATIENT_ID, PROBESET, SUBJECT_ID, RAW_INTENSITY FROM DEAPP.DE_SUBJECT_MICROARRAY_DATA WHERE TRIAL_NAME = 'eclipse2' AND PATIENT_ID IN (1004001466,	1004002963,	1004003247,	1004005504,	1004009427,	1004012443,	1004014804,	1004015405,	1004015902,	1004023082,	1004024000,	1004026404,	1004028661,	1004029240,	1004030940,	1004032699,	1004036950,	1004038458,	1004039639,	1004040557,	1004040819,	1004041759,	1004043720,	1004044469,	1004045977,	1004046431,	1004046556,	1004047737,	1004048064,	1004050015,	1004051523,	1004051534,	1004051797,	1004055255,	1004060856,	1004066561,	1004070484,	1004073342,	1004074691,	1004074976,	1004075893,	1004077210,	1004077811,	1004082970,	1004083855,	1004084729,	1004085057,	1004085352,	1004085931,	1004085953,	1004087472,	1004087587,	1004091669,	1004092291,	1004092849,	1004093472,	1004095106,	1004097067,	1004098007,	1004099958);");
			ResultSet rs = stmt.executeQuery(sql);

			//ResultSet rs = stmt.executeQuery("SELECT * FROM deapp.de_subject_microarray_data");
			//System.out.println("return time is " + (System.currentTimeMillis() - ts));
			int count = 0;
			while (rs.next()) {
			    count++;
			    if (count % 500000 == 0)
			    	System.out.println(count);
			}
			System.out.println("total number is " + count);
			System.out.println("total time is " + (System.currentTimeMillis() - ts));
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

}
