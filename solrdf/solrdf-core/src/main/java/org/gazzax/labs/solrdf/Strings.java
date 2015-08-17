package org.gazzax.labs.solrdf;

/**
 * Booch utility for string manipulation.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class Strings {
	public final static String EMPTY_STRING = "";
	
	/**
	 * Checks if the given string is not null or (not) empty string.
	 * 
	 * @param value the string to check.
	 * @return true if the string is not null and is not empty string
	 */
	public static boolean isNotNullOrEmptyString(final String value) {
		return value != null && value.trim().length() != 0;
	}
	
	/**
	 * Checks if the given string is null or empty string.
	 * 
	 * @param value the string to check.
	 * @return true if the string is not null and is not empty string
	 */
	public static boolean isNullOrEmpty(final String value) {
		return value == null || value.trim().length() == 0;
	}	
	
	/**
	 * Removes unnecessary decimal zeros from a numeric string.
	 * As an extreme case, where all decimals are 0, it ends with an integer string (e.g 10.000 = 10)
	 * 
	 * @param numericStringValue the numeric string.
	 * @return a new string with unnecessary decimal zeros removed.
	 */
	public static String round(final String numericStringValue) {
		final int indexOfDot = numericStringValue.indexOf(".");
		if (indexOfDot == -1) {
			return numericStringValue;
		}
		
		final String d = numericStringValue.substring(indexOfDot + 1);
		for (int index = 0; index < d.length(); index++) {
			if (d.charAt(index) != '0') {
				return numericStringValue;
			}
		}
		return numericStringValue.substring(0, indexOfDot);			
	}
}