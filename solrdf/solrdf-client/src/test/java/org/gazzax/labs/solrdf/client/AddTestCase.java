package org.gazzax.labs.solrdf.client;

import static org.gazzax.labs.solrdf.client.TestUtility.sampleSourceFile;
import static org.gazzax.labs.solrdf.client.TestUtility.sampleStatements;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.FileInputStream;
import java.io.FileReader;
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
	
	// TODO: in case of single statements, it would be great to have something like
	// ConcurrentUpdateSolrServer, I mean, the buffered behaviour with the asynch loader in background
	
	/**
	 * SolRDF client API must allow to add a single statement to the default graph.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void addStatement() throws Exception {
		solrdf.add(sampleStatements().iterator().next());
		
		assertTrue(solrdf.model().contains(sampleStatements().iterator().next()));
		verify(dataset).add(solrdf.model());
	}
	
	/**
	 * SolRDF client API must allow to add an array of statements to a named graph.
	 * 
	 * @throws Exception never otherwise the test will fail.
	 */
	@Test
	public void addStatementToNamedGraph() throws Exception {
		solrdf.add(uri, sampleStatements().iterator().next());
		
		assertTrue(solrdf.model(uri).contains(sampleStatements().iterator().next()));
		verify(dataset).add(uri, solrdf.model(uri));
	}		
	
	/**
	 * SolRDF client API must allow to add a single statement to the default graph.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void addSPO() throws Exception {
		final Statement statement = sampleStatements().iterator().next();
		solrdf.add(
				statement.getSubject(), 
				statement.getPredicate(), 
				statement.getObject());
		
		assertTrue(solrdf.model().contains(sampleStatements().iterator().next()));
		verify(dataset).add(solrdf.model());
	}
	
	/**
	 * SolRDF client API must allow to add an array of statements to a named graph.
	 * 
	 * @throws Exception never otherwise the test will fail.
	 */
	@Test
	public void addSPOToNamedGraph() throws Exception {
		final Statement statement = sampleStatements().iterator().next();
		solrdf.add(
				uri,
				statement.getSubject(), 
				statement.getPredicate(), 
				statement.getObject());
		
		assertTrue(solrdf.model(uri).contains(sampleStatements().iterator().next()));
		verify(dataset).add(uri, solrdf.model(uri));
	}		
	
	/**
	 * SolRDF client API must allow to add the content taken from a URL to the default graph.
	 * 
	 * @throws Exception never otherwise the test will fail.
	 */
	@Test
	public void loadDefaultGraphFromURL() throws Exception {
		solrdf.add(sampleSourceFile().getAbsolutePath(), "N-TRIPLES");
		
		assertTrue(solrdf.model().containsAll(new StmtIteratorImpl(sampleStatements().iterator())));
		verify(dataset).add(solrdf.model());
	}

	/**
	 * SolRDF client API must allow to add the content taken from a URL to a named graph.
	 * 
	 * @throws Exception never otherwise the test will fail.
	 */
	public void loadNamedGraphFromURL() throws Exception {
		solrdf.add(uri, sampleSourceFile().getAbsolutePath(), "N-TRIPLES");
		
		assertTrue(solrdf.model(uri).containsAll(new StmtIteratorImpl(sampleStatements().iterator())));
		verify(dataset).add(uri, solrdf.model(uri));
	}
	
	/**
	 * SolRDF client API must allow to add the content taken from a stream to the default graph.
	 * 
	 * @throws Exception never otherwise the test will fail.
	 */
	@Test
	public void loadDefaultGraphFromInputStream() throws Exception {
		solrdf.add(new FileInputStream(sampleSourceFile()), "N-TRIPLES");
		
		assertTrue(solrdf.model().containsAll(new StmtIteratorImpl(sampleStatements().iterator())));
		verify(dataset).add(solrdf.model());
	}	

	/**
	 * SolRDF client API must allow to add the content taken from a stream to a named graph.
	 * 
	 * @throws Exception never otherwise the test will fail.
	 */
	@Test
	public void loadNamedGraphFromInputStream() throws Exception {
		solrdf.add(uri, new FileInputStream(sampleSourceFile()), "N-TRIPLES");
		
		assertTrue(solrdf.model(uri).containsAll(new StmtIteratorImpl(sampleStatements().iterator())));
		verify(dataset).add(uri, solrdf.model(uri));
	}
	
	/**
	 * SolRDF client API must allow to add the content taken from a character stream to the default graph.
	 * 
	 * @throws Exception never otherwise the test will fail.
	 */
	@Test
	public void loadDefaultGraphFromReader() throws Exception{
		solrdf.add(new FileReader(sampleSourceFile()), "N-TRIPLES");
		
		assertTrue(solrdf.model().containsAll(new StmtIteratorImpl(sampleStatements().iterator())));
		verify(dataset).add(solrdf.model());		
	}

	/**
	 * SolRDF client API must allow to add the content taken from a character stream to a named graph.
	 * 
	 * @throws Exception never otherwise the test will fail.
	 */
	@Test
	public void loadNamedGraphFromReader() throws Exception {
		solrdf.add(uri,new FileInputStream(sampleSourceFile()), "N-TRIPLES");
		
		assertTrue(solrdf.model(uri).containsAll(new StmtIteratorImpl(sampleStatements().iterator())));
		verify(dataset).add(uri, solrdf.model(uri));		
	}
}