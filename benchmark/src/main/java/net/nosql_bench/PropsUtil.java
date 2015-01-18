package net.nosql_bench;

public class PropsUtil {

	public static int expandInt(String strInt) {

		if (strInt.endsWith("k")) {
			return 1000 * Integer.valueOf(strInt.substring(0, strInt.length()-1));
		} else if (strInt.endsWith("M")) {
			return 1_000_000 * Integer.valueOf(strInt.substring(0, strInt.length()-1));
		} else {
			return Integer.valueOf(strInt);
		}
	}
}
