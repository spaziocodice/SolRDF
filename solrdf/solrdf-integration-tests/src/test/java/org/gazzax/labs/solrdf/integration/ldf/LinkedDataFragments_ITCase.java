/*
 * This Test case makes use of some examples from 
 * 
 * "Learning SPARQL - Querying and Updating with SPARQL 1.1" by Bob DuCharme
 * 
 * Publisher: O'Reilly
 * Author: Bob DuCharme
 * ISBN: 978-1-449-37143-2
 * http://www.learningsparql.com/
 * http://shop.oreilly.com/product/0636920030829.do
 * 
 * We warmly appreciate and thank the author and O'Reilly for such permission.
 * 
 */
package org.gazzax.labs.solrdf.integration.ldf;

import static org.gazzax.labs.solrdf.MisteryGuest.misteryGuest;

import java.io.File;

import org.gazzax.labs.solrdf.integration.IntegrationTestSupertypeLayer;
import org.junit.Test;

import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
 
/**
 * Linked Data Fragments interface integration tests.
 * 
 * Note: the SolRDF client is not yet ready from a full understanding of the LDF protocol perspective.
 *  
 * @author Andrea Gazzarini
 * @since 1.0
 */  
public class LinkedDataFragments_ITCase extends IntegrationTestSupertypeLayer {
	@Test
	public void availableDatasets() throws Exception {
		load(misteryGuest(null, "sample-datasets.nt"));
		Thread.sleep(10000000000000000L);
		final Model response = SOLRDF_CLIENT.ldf().availableDatasets();
		final Model expectedResponse = ModelFactory.createDefaultModel().read(
				new File(examplesDirectory(), 
						"available-datasets-expected-response.nt").toURI().toString());
		
		assertTrue(response.isIsomorphicWith(expectedResponse));
	}
	
	@Override
	protected String examplesDirectory() {
		return "src/test/resources/ldf";
	}	
}