package org.gazzax.labs.solrdf.client;

/**
 * Thrown in case a failure is met while building the SolRDF proxy.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class UnableToBuildSolRDFClientException extends Exception {
	private static final long serialVersionUID = -663687925040115024L;
	
	/**
	 * Builds a new exception with the given cause.
	 * 
	 * @param throwable the underlying cause.
	 */
	public UnableToBuildSolRDFClientException(final Throwable throwable) {
		super(throwable);
	}
}