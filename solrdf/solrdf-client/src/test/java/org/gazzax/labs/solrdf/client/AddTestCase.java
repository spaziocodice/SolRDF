package org.gazzax.labs.solrdf.client;

import static org.gazzax.labs.solrdf.client.TestUtility.invalidPath;
import static org.gazzax.labs.solrdf.client.TestUtility.sampleSourceFile;
import static org.gazzax.labs.solrdf.client.TestUtility.sampleStatements;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.solr.client.solrj.SolrServer;
import org.junit.Before;
import org.junit.Ignore;
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
@Ignore
public class AddTestCase {
	String uri;
	DatasetAccessor dataset;
	SolrServer solr;
	SolRDF solrdf;

	/**
	 * Setup fixture for this test case.
	 * 
	 * @throws Exception never, otherwise the corresponding test fails.
	 */
	@Before
	public void setUp() throws Exception{
		dataset = mock(DatasetAccessor.class);
		solr = mock(SolrServer.class);
		solrdf = new SolRDF(dataset, "/sparql", solr);
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
	 * If an add* method fails, a dedicated exception is raised.
	 * 
	 * @throws Exception never otherwise the test will fail.
	 */
	@Test
	public void addListOfStatementsWithFailure() throws Exception {
		doThrow(new RuntimeException()).when(dataset).add(solrdf.model());
		try {
			solrdf.add(sampleStatements());
			fail();
		} catch (final UnableToAddException expected) {
			// NOthing to be done, this is the expected behaviour
		}
		
		
		doThrow(new RuntimeException()).when(dataset).add(uri, solrdf.model(uri));

		try {
			solrdf.add(uri, sampleStatements());
			fail();
		} catch (final UnableToAddException expected) {
			// NOthing to be done, this is the expected behaviour
		}		
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
	
	/**
	 * If an add* method fails, a dedicated exception is raised.
	 * 
	 * @throws Exception never otherwise the test will fail.
	 */
	@Test
	public void addArrayOfStatementsWithFailure() throws Exception {
		doThrow(new RuntimeException()).when(dataset).add(solrdf.model());
		try {
			solrdf.add(sampleStatements().toArray(new Statement[0]));
			fail();
		} catch (final UnableToAddException expected) {
			// NOthing to be done, this is the expected behaviour
		}
		
		
		doThrow(new RuntimeException()).when(dataset).add(uri, solrdf.model(uri));

		try {
			solrdf.add(uri, sampleStatements().toArray(new Statement[0]));
			fail();
		} catch (final UnableToAddException expected) {
			// NOthing to be done, this is the expected behaviour
		}		
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
	 * If an add* method fails, a dedicated exception is raised.
	 * 
	 * @throws Exception never otherwise the test will fail.
	 */
	@Test
	public void addStatementWithFailure() throws Exception {
		doThrow(new RuntimeException()).when(dataset).add(solrdf.model());
		try {
			solrdf.add(sampleStatements().iterator().next());
			fail();
		} catch (final UnableToAddException expected) {
			// NOthing to be done, this is the expected behaviour
		}
		
		
		doThrow(new RuntimeException()).when(dataset).add(uri, solrdf.model(uri));

		try {
			solrdf.add(uri, sampleStatements().iterator().next());
			fail();
		} catch (final UnableToAddException expected) {
			// NOthing to be done, this is the expected behaviour
		}		
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
	 * If an add* method fails, a dedicated exception is raised.
	 * 
	 * @throws Exception never otherwise the test will fail.
	 */
	@Test
	public void addSPOWithFailure() throws Exception {
		final Statement statement = sampleStatements().iterator().next();
		doThrow(new RuntimeException()).when(dataset).add(solrdf.model());
		try {
			solrdf.add(
					statement.getSubject(), 
					statement.getPredicate(), 
					statement.getObject());
			fail();
		} catch (final UnableToAddException expected) {
			// NOthing to be done, this is the expected behaviour
		}
		
		
		doThrow(new RuntimeException()).when(dataset).add(uri, solrdf.model(uri));

		try {
			solrdf.add(
					uri,
					statement.getSubject(), 
					statement.getPredicate(), 
					statement.getObject());
			fail();
		} catch (final UnableToAddException expected) {
			// NOthing to be done, this is the expected behaviour
		}		
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
	@Test
	public void loadNamedGraphFromURL() throws Exception {
		solrdf.add(uri, sampleSourceFile().getAbsolutePath(), "N-TRIPLES");
		
		assertTrue(solrdf.model(uri).containsAll(new StmtIteratorImpl(sampleStatements().iterator())));
		verify(dataset).add(uri, solrdf.model(uri));
	}
	
	/**
	 * In case the given URL is not valid then an exception must be thrown.
	 * 
	 * @throws Exception never otherwise the test will fail.
	 */
	@Test
	public void invalidURL() throws Exception {
		try {
			solrdf.add(uri, invalidPath(), "N-TRIPLES");
			fail();
		} catch (final UnableToAddException expected) {
			// FIXME: more specialised exception
		}
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
	
	/**
	 * SolRDF client API must allow explicit commit of pending changes.
	 * 
	 * @throws Exception never otherwise the test will fail.
	 */
	@Test
	public void commit() throws Exception {
		final boolean waitSearcher = true;
		final boolean waitFlush = true;
		final boolean softCommit = true;
		
		solrdf.commit();
		verify(solr).commit();	
		
		solrdf.commit(waitFlush, waitSearcher);		
		verify(solr).commit(waitFlush, waitSearcher);	
		
		solrdf.commit(waitFlush, waitSearcher, softCommit);		
		verify(solr).commit(waitFlush, waitSearcher, softCommit);	
	}
	
	/**
	 * Any exception raised during a commit will be indicated by a thrown UnableToCommitException.
	 * 
	 * @throws Exception never otherwise the test will fail.
	 */
	@Test
	public void commitWithFailure() throws Exception {
		try {
			when(solr.commit()).thenThrow(new IOException());
			
			solrdf.commit();
			fail();
		} catch (UnableToCommitException expected) {
			// Nothing, this is the expected behaviour
		}	
		
		try {
			when(solr.commit(false, true)).thenThrow(new IOException());
			
			solrdf.commit(false, true);		
			fail();
		} catch (UnableToCommitException expected) {
			// Nothing, this is the expected behaviour
		}	
		
		try {
			when(solr.commit(false, true, true)).thenThrow(new IOException());

			solrdf.commit(false, true, true);		
			fail();
		} catch (UnableToCommitException expected) {
			// Nothing, this is the expected behaviour
		}	
	}	
}