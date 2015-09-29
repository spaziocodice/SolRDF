package org.gazzax.labs.solrdf.integration.faceting;

import static org.gazzax.labs.solrdf.TestUtility.DUMMY_BASE_URI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.util.NamedList;
import org.gazzax.labs.solrdf.integration.IntegrationTestSupertypeLayer;
import org.junit.Before;
import org.junit.BeforeClass;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * Supertype layer for all faceting test cases.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class FacetTestSupertypeLayer extends IntegrationTestSupertypeLayer {
	private final static String TEST_DATA_URI = new File("src/test/resources/sample_data/faceting_test_dataset.nt").toURI().toString();
	protected SolrQuery query;
	
	protected final String downloadsQuery = "p:<http\\://example.org/ns#downloads>";
	protected final String priceQuery = "p:<http\\://example.org/ns#price>";
	protected final String dateQuery = "p:<http\\://purl.org/dc/elements/1.1/date>";
	
	/**
	 * Loads all triples found in the datafile associated with the given name.
	 * 
	 * @throws Exception hopefully never, otherwise the test fails.
	 */
	@BeforeClass
	public final static void loadSampleData() throws Exception {
		final Model memoryModel = ModelFactory.createDefaultModel();
		memoryModel.read(TEST_DATA_URI, DUMMY_BASE_URI, "N-TRIPLES");
  
		SOLRDF.add(memoryModel.listStatements());
		SOLRDF.commit();
		
		final Model model = SOLRDF.getDefaultModel();
		  
		assertFalse(model.isEmpty());
		assertTrue(model.isIsomorphicWith(memoryModel));
	}
	
	/**
	 * Setup fixture for this test.
	 * 
	 * @throws Exception hopefully never, otherwise the test fails.
	 */
	@Before
	public void init() throws Exception {
		query = new SolrQuery("SELECT * WHERE { ?s ?p ?o }");
		query.setRows(0);
		query.setFacet(true);
		query.setRequestHandler("/sparql");
	}
	
	@Override
	protected String examplesDirectory() {
		throw new IllegalStateException();
	}	
	
	/**
	 * Asserts a given NamedList against an expected map of results.
	 * 
	 * @param expected the map containing expected (facet) results.
	 * @param actual the actual {@link NamedList} returned from Solr.
	 */
	protected void assertFacetResults(final Map<String, Integer> expected, final NamedList<?> actual) {
		assertNotNull(actual);
		assertEquals(expected.size(), actual.size());
		
		for (final Entry<String, Integer> expectedCount : expected.entrySet()) {
			assertEquals(expectedCount.getValue(), actual.remove(expectedCount.getKey()));
		}
		assertEquals(0, actual.size());
	}		
}
