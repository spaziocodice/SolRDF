package org.gazzax.labs.solrdf;

/**
 * Booch utility for string manipulation.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class Strings {
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