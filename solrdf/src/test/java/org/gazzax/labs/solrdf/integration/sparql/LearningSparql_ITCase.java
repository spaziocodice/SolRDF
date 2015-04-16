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
package org.gazzax.labs.solrdf.integration.sparql;

import static org.gazzax.labs.solrdf.MisteryGuest.misteryGuest;
import static org.gazzax.labs.solrdf.TestUtility.DUMMY_BASE_URI;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.gazzax.labs.solrdf.MisteryGuest;
import org.gazzax.labs.solrdf.integration.IntegrationTestSupertypeLayer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetAccessor;
import com.hp.hpl.jena.query.DatasetAccessorFactory;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.resultset.ResultSetCompare;

/**
 * Facet Object Queries integration test.
 *  
 * @author Andrea Gazzarini
 * @since 1.0
 */  
public class LearningSparql_ITCase extends IntegrationTestSupertypeLayer {
	protected final static String LEARNING_SPARQL_EXAMPLES_DIR = "src/test/resources/LearningSPARQLExamples";
	
	protected Dataset memoryDataset;
	protected DatasetAccessor dataset;
	protected QueryExecution execution;
	protected QueryExecution inMemoryExecution;
	
	static final List<MisteryGuest> DATA = new ArrayList<MisteryGuest>();
	
	/**
	 * Setup fixture for this test.
	 */
	@Before
	public final void setUp() {
		memoryDataset = DatasetFactory.createMem();
		dataset = DatasetAccessorFactory.createHTTP(GRAPH_STORE_ENDPOINT_URI);
	}
	 
	/** 
	 * Shutdown procedure for this test.
	 * 
	 * @throws Exception hopefully never.
	 */
	@After
	public void tearDown() throws Exception {
		clearDatasets();
		execution.close();
		inMemoryExecution.close();
	}	

	@Test
	public void queryWithPrefixes_I() throws Exception {
		execute(misteryGuest("ex003.rq", "ex002.ttl"));		
	}
	
	@Test
	public void queryWithPrefixes_II() throws Exception {
		execute(misteryGuest("ex006.rq", "ex002.ttl"));		
	}

	@Test
	public void fromKeyword() throws Exception {
		execute(misteryGuest("ex007.rq", "ex002.ttl"));		
	}

	@Test
	public void queryWithOneVariable() throws Exception {
		execute(misteryGuest("ex008.rq", "ex002.ttl"));		
	}

	@Test
	public void queryWithTwoVariables() throws Exception {
		execute(misteryGuest("ex010.rq", "ex002.ttl"));		
	}

	@Test
	public void multipleTriplePatterns_I() throws Exception {
		execute(misteryGuest("ex013.rq", "ex012.ttl"));		
	}

	@Test
	public void multipleTriplePatterns_II() throws Exception {
		execute(misteryGuest("ex015.rq", "ex012.ttl"));		
	}

	@Test
	public void humanReadableAnswersWithLabels() throws Exception {
		execute(misteryGuest("ex017.rq", "ex012.ttl"));		
	}

	@Test
	public void entityDescriptionQuery() throws Exception {
		execute(misteryGuest("ex019.rq", "ex012.ttl"));		
	}

	@Test
	public void regularExpression() throws Exception {
		execute(misteryGuest("ex021.rq", "ex012.ttl"));		
	}

	@Test
	public void selectFirstAndLastName() throws Exception {
		execute(misteryGuest("ex047.rq", "ex012.ttl"));		
	}

	@Test
	public void queryingFOAFLabels() throws Exception {
		execute(misteryGuest("ex052.rq", "ex050.ttl", "foaf.rdf"));		
	}
	
	@Test
	public void dataThatMightBeNotThere() throws Exception {
		execute(misteryGuest("ex055.rq", "ex054.ttl"));		
	}	

	@Test
	public void optionalKeyword() throws Exception {
		execute(misteryGuest("ex057.rq", "ex054.ttl"));		
	}	

	@Test
	public void optionalGraphPattern() throws Exception {
		execute(misteryGuest("ex059.rq", "ex054.ttl"));		
	}	

	@Test
	public void optionalGraphPatterns() throws Exception {
		execute(misteryGuest("ex061.rq", "ex054.ttl"));		
	}	

	@Test
	public void orderOfOptionalGraphPatterns() throws Exception {
		execute(misteryGuest("ex063.rq", "ex054.ttl"));		
	}	

	@Test
	public void boundKeyword() throws Exception {
		execute(misteryGuest("ex065.rq", "ex054.ttl"));		
	}	

	@Test
	public void filterNotExists() throws Exception {
		execute(misteryGuest("ex067.rq", "ex054.ttl"));		
	}	

	@Test
	public void minusKeyword() throws Exception {
		execute(misteryGuest("ex068.rq", "ex054.ttl"));		
	}	

	@Test
	public void multipleTables() throws Exception {
		execute(misteryGuest("ex070.rq", "ex069.ttl"));		
	}	

	@Test
	public void multipleTablesWithSplitDatasets() throws Exception {
		execute(misteryGuest("ex070.rq", "ex072.ttl", "ex073.ttl", "ex368.ttl"));		
	}	

	@Test
	public void bindEither_I() throws Exception {
		execute(misteryGuest("ex075.rq", "ex074.ttl"));		
	}	

	private void execute(final MisteryGuest data) throws Exception {
		load(data);
		
		final Query query = QueryFactory.create(queryString(data.query));
		try {
			assertTrue(
					Arrays.toString(data.datasets) + ", " + data.query,
					ResultSetCompare.isomorphic(
							(execution = QueryExecutionFactory.sparqlService(SPARQL_ENDPOINT_URI, query)).execSelect(),
							(inMemoryExecution = QueryExecutionFactory.create(query, memoryDataset)).execSelect()));
		} catch (final Exception error) {
			QueryExecution debugExecution = null;
			log.debug("JNS\n" + ResultSetFormatter.asText(
					(debugExecution = QueryExecutionFactory.sparqlService(SPARQL_ENDPOINT_URI, query)).execSelect()));
			
			debugExecution.close();
			log.debug("MEM\n" + ResultSetFormatter.asText(
					(debugExecution = (QueryExecutionFactory.create(query, memoryDataset))).execSelect()));
			
			debugExecution.close();
			throw error;
		} 
	}
	
	/**
	 * Reads a query from the file associated with this test and builds a query string.
	 * 
	 * @param filename the filename.
	 * @return the query string associated with this test.
	 * @throws IOException in case of I/O failure while reading the file.
	 */
	protected String queryString(final String filename) throws IOException {
		return readFile(filename);
	}
	
	/**
	 * Builds a string from a given file.
	 * 
	 * @param filename the filename (without path).
	 * @return a string with the file content.
	 * @throws IOException in case of I/O failure while reading the file.
	 */
	protected String readFile(final String filename) throws IOException {
		return new String(Files.readAllBytes(Paths.get(source(filename))));
	}	
	
	/**
	 * Loads all triples found in the datafile associated with the given name.
	 * 
	 * @param datafileName the name of the datafile.
	 * @throws Exception hopefully never, otherwise the test fails.
	 */
	protected void load(final MisteryGuest data) throws Exception {
		final Model memoryModel = memoryDataset.getDefaultModel();
				 
		for (final String datafileName : data.datasets) {
			final String dataURL = source(datafileName).toString();
			final String lang = datafileName.endsWith("ttl") ? "TTL" : null;
			memoryModel.read(dataURL, DUMMY_BASE_URI, lang);
		}  
  
		dataset.add(memoryModel);
		commitChanges();
		
		final Model model = dataset.getModel();
		  
		assertFalse(Arrays.toString(data.datasets) + ", " + data.query, model.isEmpty());
		assertTrue(Arrays.toString(data.datasets) + ", " + data.query, model.isIsomorphicWith(memoryModel));
	} 
 
	/**
	 * Returns the URI of a given filename.
	 * 
	 * @param filename the filename.
	 * @return the URI (as string) of a given filename.
	 */ 
	URI source(final String filename) {
		return new File(LEARNING_SPARQL_EXAMPLES_DIR, filename).toURI();
	}	
	
	/**
	 * Removes all data created by this test.
	 * 
	 * @throws Exception hopefully never.
	 */
	private void clearDatasets() throws Exception {
		clearData();
		memoryDataset.getDefaultModel().removeAll();
	}
}