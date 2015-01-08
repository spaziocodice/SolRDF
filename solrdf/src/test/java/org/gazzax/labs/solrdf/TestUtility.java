package org.gazzax.labs.solrdf;

/**
 * A bunch of test utilities.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */ 
public abstract class TestUtility {
	public static final String DUMMY_BASE_URI = "http://example.org/";
	
	/**
	 * Waits for a second.
	 */
	public static void eheh() {
		try {
			Thread.sleep(2000);
		} catch (final Exception ignore) {
			// Ignore
		}
	}
}