package org.gazzax.labs.solrdf;

import com.hp.hpl.jena.graph.Node;

/**
 * A simple value object encapsulating data (i.e. filenames) for a given test.
 * 
 * @author Andrea Gazzarini
 * @since 1.0  
 */     
public final class MisteryGuest {
	public final String [] datasets;
	public final String query;
	public final String graphURI;   

	/**
	 * Builds a new test bundle with the given data.
	 * 
	 * @param datasetsFilenames one or more datafile that contains data.
	 * @param graphURI the target graphURI.
	 * @param queryFilename the name of the file containing the SPARQL query for a given test.
	 */
	private MisteryGuest(final String queryFilename, final String graphURI, final String ... datasetsFilenames) {
		this.datasets = datasetsFilenames;
		this.query = queryFilename;
		this.graphURI = graphURI;
	} 

	/**
	 * Factory method. 
	 * 
	 * @param datasetsFilenames one or more datafile that contains data.
	 * @param queryFilename the name of the file containing the SPARQL query for a given test.
	 * @return new {@link MisteryGuest} instance.
	 */
	public static MisteryGuest misteryGuest(final String queryFilename, final String ... datasetsFilenames) {
		return new MisteryGuest(queryFilename, null, datasetsFilenames);
	}
	
	/**
	 * Factory method. 
	 * 
	 * @param datasetsFilenames one or more datafile that contains data.
	 * @param graphURI the target graphURI.
	 * @param queryFilename the name of the file containing the SPARQL query for a given test.
	 * @return new {@link MisteryGuest} instance.
	 */
	public static MisteryGuest misteryGuest(final String queryFilename, final Node graphURI, final String ... datasetsFilenames) {
		return new MisteryGuest(queryFilename, graphURI.getURI(), datasetsFilenames);
	}	
}