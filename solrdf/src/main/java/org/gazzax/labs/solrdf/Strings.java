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
		return indexOfDot != -1 ? numericStringValue.substring(0, indexOfDot) : numericStringValue;
	}
}