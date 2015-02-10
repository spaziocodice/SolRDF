package org.gazzax.labs.solrdf;

import java.util.Random;

/**
 * A bunch of test utilities.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */       
public abstract class TestUtility {
	public static final Random RANDOMIZER = new Random();
	public static final String DUMMY_BASE_URI = "http://example.org/";
	
	/**
	 * Returns a pseudorandom string.
	 * 
	 * @return a pseudorandom string.
	 */
	public static String randomString() {
		return String.valueOf(System.currentTimeMillis() + RANDOMIZER.nextLong());
	}
}   