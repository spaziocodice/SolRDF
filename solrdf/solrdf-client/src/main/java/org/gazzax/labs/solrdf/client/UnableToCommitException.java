package org.gazzax.labs.solrdf.client;

/**
 * Thrown in case of a failure during a commit command.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class UnableToCommitException extends Exception {

	private static final long serialVersionUID = -5122995061561828016L;
	
	/**
	 * Builds a new exception with the given cause.
	 * 
	 * @param throwable the exception cause.
	 */
	public UnableToCommitException(final Throwable throwable) {
		super(throwable);
	}
}