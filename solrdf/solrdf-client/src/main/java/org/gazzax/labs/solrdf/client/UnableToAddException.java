package org.gazzax.labs.solrdf.client;

/**
 * Thrown in case of a failure during an add command.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class UnableToAddException extends Exception {

	private static final long serialVersionUID = -5122995061561828016L;
	
	/**
	 * Builds a new exception with the given cause.
	 * 
	 * @param throwable the exception cause.
	 */
	public UnableToAddException(final Throwable throwable) {
		super(throwable);
	}
}