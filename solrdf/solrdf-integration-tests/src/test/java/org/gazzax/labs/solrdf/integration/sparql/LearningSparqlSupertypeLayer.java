package org.gazzax.labs.solrdf.integration.sparql;

import org.gazzax.labs.solrdf.integration.IntegrationTestSupertypeLayer;

/**
 * Supertype layer for all "Learning SPARQL" integration tests.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class LearningSparqlSupertypeLayer extends IntegrationTestSupertypeLayer {
	protected final static String LEARNING_SPARQL_EXAMPLES_DIR = "src/test/resources/LearningSPARQLExamples";
	
	@Override
	protected String examplesDirectory() {
		return LEARNING_SPARQL_EXAMPLES_DIR;
	}
}