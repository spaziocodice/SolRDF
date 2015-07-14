package org.gazzax.labs.solrdf.client;

import static org.gazzax.labs.solrdf.client.TestUtility.sampleStatements;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.net.URI;

import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.query.DatasetAccessor;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.impl.StmtIteratorImpl;

/**
 * Test case which elencates several ways for adding data to SolRDF.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class AddTestCase {
	String uri;
	DatasetAccessor dataset;
	SolRDF solrdf;

	/**
	 * Setup fixture for this test case.
	 * 
	 * @throws Exception never, otherwise the corresponding test fails.
	 */
	@Before
	public void setUp() throws Exception{
		dataset = mock(DatasetAccessor.class);
		solrdf = new SolRDF(dataset);
		uri = new URI("http://org.example.blablabla").toString();
	}
	
	/**
	 * SolRDF client API must allow to add a list of statements to the default graph.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void addListOfStatements() throws Exception {
		solrdf.add(sampleStatements());

		assertTrue(solrdf.model().containsAll(new StmtIteratorImpl(sampleStatements().iterator())));
		verify(dataset).add(solrdf.model());
	}
	
	/**
	 * SolRDF client API must allow to add a list of statements to a named graph.
	 * 
	 * @throws Exception never otherwise the test will fail.
	 */
	@Test
	public void addListOfStatementsToNamedGraph() throws Exception {
		solrdf.add(uri, sampleStatements());
		
		assertTrue(solrdf.model(uri).containsAll(new StmtIteratorImpl(sampleStatements().iterator())));
		verify(dataset).add(uri, solrdf.model(uri));
	}	
	
	/**
	 * SolRDF client API must allow to add an array of statements to the default graph.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void addArrayOfStatements() throws Exception {
		solrdf.add(sampleStatements().toArray(new Statement[0]));
		
		assertTrue(solrdf.model().containsAll(new StmtIteratorImpl(sampleStatements().iterator())));
		verify(dataset).add(solrdf.model());
	}
	
	/**
	 * SolRDF client API must allow to add an array of statements to a named graph.
	 * 
	 * @throws Exception never otherwise the test will fail.
	 */
	@Test
	public void addArrayOfStatementsToNamedGraph() throws Exception {
		solrdf.add(uri, sampleStatements().toArray(new Statement[0]));
		
		assertTrue(solrdf.model(uri).containsAll(new StmtIteratorImpl(sampleStatements().iterator())));
		verify(dataset).add(uri, solrdf.model(uri));
	}		
}