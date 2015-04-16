package org.gazzax.labs.solrdf;

/**
 * A simple value object encapsulating data (i.e. filenames) for a given test.
 * 
 * @author Andrea Gazzarini
 * @since 1.0  
 */     
public final class MisteryGuest {
	public final String [] datasets;
	public final String query;
	   
	/**
	 * Builds a new test bundle with the given data.
	 * 
	 * @param datasetsFilenames one or more datafile that contains data.
	 * @param queryFilename the name of the file containing the SPARQL query for a given test.
	 */
	private MisteryGuest(final String queryFilename, final String ... datasetsFilenames) {
		this.datasets = datasetsFilenames;
		this.query = queryFilename;
	} 
	
	/**
	 * Factory method. 
	 * 
	 * @param datasetsFilenames one or more datafile that contains data.
	 * @param queryFilename the name of the file containing the SPARQL query for a given test.
	 * @return new {@link MisteryGuest} instance.
	 */
	public static MisteryGuest misteryGuest(final String queryFilename, final String ... datasetsFilenames) {
		return new MisteryGuest(queryFilename, datasetsFilenames);
	}
}