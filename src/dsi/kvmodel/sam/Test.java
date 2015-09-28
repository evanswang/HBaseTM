package dsi.kvmodel.sam;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/*
		String headStr = "abc:def";
		System.out.println(headStr.indexOf(':'));
		System.out.println(headStr.substring(0, headStr.indexOf(':')));
		System.out.println(headStr.substring(headStr.indexOf(':') + 1, headStr.length()));
		*/
		/*
		String cigar = "3M7=5H8=";
		cigar = cigar.replace('=', 'E');
		int i = 0;
		int length = 0;
		while (i < cigar.length()) {
			if (cigar.charAt(i) > '9') {
				switch (cigar.charAt(i)) {
				case 'M':
				case 'D':
				case 'N':
				case 'E':
				case 'X':
				case 'P':
					length += Integer.parseInt(cigar.substring(0, i));
				}
				cigar = cigar.substring(i + 1);
				i = 0;
			}
			i++;
		}*/
		long a = 123456;
		System.out.println(String.format("%09d", a));

	}

}
