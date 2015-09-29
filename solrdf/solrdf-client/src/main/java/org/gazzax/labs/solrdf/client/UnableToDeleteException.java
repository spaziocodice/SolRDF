package org.gazzax.labs.solrdf.client;

/**
 * Thrown in case a deletion cannot be executed (or it raises some exception).
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class UnableToDeleteException extends Exception {
	private static final long serialVersionUID = 1L;
	
	/**
	 * Builds a new exception with the given cause.
	 * 
	 * @param throwable the exception cause.
	 */
	public UnableToDeleteException(final Throwable throwable) {
		super(throwable);
	}
}