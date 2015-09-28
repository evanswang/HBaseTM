package dsi.kvmodel.sam;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.StringTokenizer;


public class TestBAM {
	static final int PORT = 6666;
	
	private String loadStream(InputStream in) throws IOException {
		int ptr = 0;
		in = new BufferedInputStream(in);
		StringBuffer buffer = new StringBuffer();
		while ((ptr = in.read()) != -1) {
			buffer.append((char) ptr);
		}
		return buffer.toString();
	}
	
	private String cmdExec(String cmd) {
		String result = "";
		InputStream in = null;
		Process ps = null;
		try {
			if (cmd == null || cmd.equals(""))
				//inner code to deal without extra command
				return result;
			String[] fullcmd = {"/bin/sh", "-c", cmd};
			ps = Runtime.getRuntime().exec(fullcmd);
			in = ps.getInputStream();
			result = loadStream(in);
		} catch (Exception e) {
			System.out.println("Bash Execution Error " + cmd);
			e.printStackTrace();
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (ps != null) {
				ps.destroy();
			}
		}
		return result;	
	}
	
	private class MultipleReader implements Runnable {
		
		private Socket socket;
		private long start;
		private long end;
		private String chrom;
		
		
		public MultipleReader (String chrom, long start, long end, Socket socket) {
			this.start = start;
			this.end = end;
			this.chrom = chrom;
			this.socket = socket;
		}
		
		public void run() {
			BufferedReader in = null;
			PrintWriter out = null;
			String line = null;
			long count = 0;
			try {
				in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				out = new PrintWriter(socket.getOutputStream(), true);
				out.println(chrom+","+start+","+end);
				//System.out.println("reads start");
				while ((line = in.readLine()) != null) {
					System.out.println(line);
					count ++;
				}
				//System.out.println("read end");
				System.out.println("read number is " + count);
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					in.close();
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				out.close();
			}
		}
		
		private void selfCheck () {
			long t1 = System.currentTimeMillis();
			long count = 0;
			String result = cmdExec("/data/sam/samcode/runBam.sh " + chrom + ":" + start + " " + end);
			StringTokenizer token = new StringTokenizer(result, "\n");
			while(token.hasMoreTokens()) {
				StringTokenizer subtoken = new StringTokenizer(token.nextToken(), "\t");
				while (subtoken.hasMoreTokens()) {
					subtoken.nextToken();
				}
				count ++;
			}
			System.out.println("total number is " + count);
			System.out.println("scan time is " + (System.currentTimeMillis() - t1));
			System.out.println("end time is " + System.currentTimeMillis());
		}
	}
	
	
	public void conBamTest(String rname, long regionLen, int threadNum) {

		long regionStart = 50000000 / threadNum;
		long start, end;
		for (int i = 0; i < threadNum; i++) {
			start = 10000000 + regionStart * i;
			end = start + regionLen;
			Socket socket;
			try {
				socket = new Socket("rmpi01", PORT);
				Runnable r = new MultipleReader(rname, start, end, socket);
				Thread t = new Thread(r);
				t.start();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
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
					.println("conscan for scan alignments overlapping a region");
			return;
		}
		
		if (args[0].equals("conscan")) {
			TestBAM test = new TestBAM();
			test.conBamTest(args[1], Long.parseLong(args[2]), Integer.parseInt(args[3]));
		} else {
			System.out.println("please input an argument");
			System.out
					.println("conscan for scan alignments overlapping a region");
			return;
		}
		System.out.println("final end time: " + System.currentTimeMillis());
	}

}
