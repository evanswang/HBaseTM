package dsi.kvmodel.sam;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;

public class BAMServer {
	private final int PORT = 6666;
	
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
				// inner code to deal without extra command
				return result;
			String[] fullcmd = { "/bin/sh", "-c", cmd };
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

	private class RespondCheck implements Runnable {

		private Socket socket;

		public RespondCheck(Socket incoming) {
			this.socket = incoming;
		}

		public void run() {

			BufferedReader in = null;
			PrintWriter out = null;
			String cmd = null;
			try {
				try {
					in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					out = new PrintWriter(socket.getOutputStream(), true);

					cmd = in.readLine();
					StringTokenizer token = new StringTokenizer(cmd, ",");
					String chrom = token.nextToken();
					String start = token.nextToken();
					String end = token.nextToken();

					long t1 = System.currentTimeMillis();
					long count = 0;
					String result = cmdExec("/data/sam/samcode/runBam.sh "
							+ chrom + ":" + start + " " + end);
					out.print(result);

					System.out.println("total number is " + count);
					System.out.println("scan time is "
							+ (System.currentTimeMillis() - t1));
					System.out.println("end time is "
							+ System.currentTimeMillis());
				} finally {
					try {
						in.close();
						socket.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					out.close();
					
				}
			} catch (Exception e) {
				System.out.println("Received Command Error" + " cmd is " + cmd
						+ " reason: " + e.getMessage());
				e.printStackTrace();
			}
		}
	}

	public void daemon() {
		try {
			ServerSocket s = new ServerSocket(PORT);

			while (true) {
				Socket incoming = s.accept();
				Runnable r = new RespondCheck(incoming);
				Thread t = new Thread(r);
				t.start();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {

		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		BAMServer server = new BAMServer();
		server.daemon();

	}

}
