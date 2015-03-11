package org.gazzax.labs.solrdf;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * {@link Strings} test case.
 *  
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class StringsTestCase {
	
	/**
	 * If a string is null or empty then this method must return true.
	 */
	@Test
	public void isNullOrEmptyString() {
		final String [] emptyValues = {"", "    ", "\n\t", null};
		for (final String emptyValue : emptyValues) {
			assertTrue(Strings.isNullOrEmptyString(emptyValue));
			assertFalse(Strings.isNotNullOrEmptyString(emptyValue));
		}
	}
	
	/**
	 * If a string is not null or empty then this method must return true.
	 */
	@Test
	public void isNotNullOrEmptyString() {
		final String [] emptyValues = {"a", " b   ", "\n\tcde"};
		for (final String emptyValue : emptyValues) {
			assertFalse(Strings.isNullOrEmptyString(emptyValue));
			assertTrue(Strings.isNotNullOrEmptyString(emptyValue));
		}
	}	
}
