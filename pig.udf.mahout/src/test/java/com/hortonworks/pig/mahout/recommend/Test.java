package com.hortonworks.pig.mahout.recommend;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String ucode = "\t";
		System.out.println(parseSingleQuotedString(ucode));
		
		
		String record = "abcd::1234::xyz::::1::";
		
		for(String field : record.split("::"))
		{
			System.out.println(field + " " + field.length());
		}

	}
	
	private static String parseSingleQuotedString(String delimiter) {
		int startIndex = 0;
		int endIndex;
		while (startIndex < delimiter.length()
				&& delimiter.charAt(startIndex++) != '\'');
		endIndex = startIndex;
		while (endIndex < delimiter.length()
				&& delimiter.charAt(endIndex) != '\'') {
			if (delimiter.charAt(endIndex) == '\\') {
				endIndex++;
			}
			endIndex++;
		}

		return (endIndex < delimiter.length()) ? delimiter.substring(
				startIndex, endIndex) : delimiter;
	}

}
