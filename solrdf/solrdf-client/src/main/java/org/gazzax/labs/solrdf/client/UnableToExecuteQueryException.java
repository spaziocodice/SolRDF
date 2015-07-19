package org.gazzax.labs.solrdf.client;

/**
 * Thrown in case of query failure.
 * The failure could be before, during or after the actual query execution.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class UnableToExecuteQueryException extends Exception {

	private static final long serialVersionUID = 289019560117959176L;
	
	/**
	 * Builds a new exception with the given cause.
	 * 
	 * @param throwable the exception cause.
	 */
	public UnableToExecuteQueryException(final Throwable cause) {
		super(cause);
	}
}
