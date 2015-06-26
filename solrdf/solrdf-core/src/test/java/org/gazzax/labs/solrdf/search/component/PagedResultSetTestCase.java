package org.gazzax.labs.solrdf.search.component;

import static org.gazzax.labs.solrdf.TestUtility.DUMMY_BASE_URI;
import static org.junit.Assert.*;

import java.io.FileInputStream;

import org.apache.jena.riot.Lang;
import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetRewindable;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import static org.mockito.Mockito.*;

/**
 * Test case for {@link PagedResultSet}.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class PagedResultSetTestCase {
	private Model model;
	
	/**
	 * Setup fixture for this test case.
	 * 
	 * @throws Exception hopefully never, otherwise the corresponding test fails.
	 */
	@Before
	public void setUp() throws Exception {
		model = ModelFactory.createDefaultModel();
		model.read(new FileInputStream(
				"../solrdf-integration-tests/src/test/resources/sample_data/bsbm-generated-dataset.nt"), 
				DUMMY_BASE_URI, 
				Lang.NTRIPLES.getLabel());
	}
	
	/**
	 * If the required size (+offset) is greater than the underlying resultset size then only the available rows will be returned. 
	 */
	@Test
	public void sizeGreaterThanEffectiveSize() {
		final ResultSetRewindable cut = new PagedResultSet(executeQuery(), 10, (int)(model.size() - 5));

		while (cut.hasNext()) {
			cut.next();
		}
		
		assertEquals(5, cut.size());
		cut.reset();
	}
	
	/**
	 * In case the required size + offset denotes an unavailable set of results, then an exception will be thrown.
	 */
	@Test
	public void outOfAvailableRange() {
		final ResultSetRewindable cut = new PagedResultSet(executeQuery(), 10, (int)(model.size() + 1));

		while (cut.hasNext()) {
			cut.next();
		}
		
		assertEquals(0, cut.size());

		cut.reset();
		
		assertEquals(0, cut.size());
	}
	
	/**
	 * In case the decorated {@link ResultSet} is null then an empty wrapper will be returned.
	 */
	@Test
	public void nullResultSet() {
		final ResultSetRewindable cut = new PagedResultSet(null, 10, 0);
		assertFalse(cut.hasNext());
	}
	
	/**
	 * With a size equals to 0, the {@link PagedResultSet} will be empty, 
	 * but at least one iteration step needs to be done on the wrapped {@link ResultSet}.
	 */
	@Test
	public void zeroSize() {
		final ResultSet rs = mock(ResultSet.class);
		when(rs.hasNext()).thenReturn(true);
		
		final ResultSetRewindable cut = new PagedResultSet(rs, 0, 10);
		
		verify(rs).next();
		
		assertFalse(cut.hasNext());
	}

	/**
	 * A negative size will be interpreted as 0.
	 */
	@Test
	public void invalidSize() {
		assertEquals(0, new PagedResultSet(mock(ResultSet.class), -12, 10).size);
	}
	
	/**
	 * A negative offset will be interpreted as 0.
	 */
	@Test
	public void invalidOffset() {
		assertEquals(0, new PagedResultSet(mock(ResultSet.class), 129, -122).offset);
	}	
	
	/**
	 * Positive test.
	 */
	@Test
	public void range() {
		final int offset = 120; 
		final int size = 24;
		
		final ResultSetRewindable cut = new PagedResultSet(executeQuery(), size, offset);
		final ResultSet resultSet = executeQuery();
		
		while (cut.hasNext()) {
			cut.next();
		}
		
		assertEquals(size, cut.size());
		cut.reset();
		
		for (int i = 0; i < offset; i++) {
			resultSet.next();
		}
		
		for (int i = 0; i < size; i++) {
			assertEquals(resultSet.nextBinding(), cut.nextBinding());
		}
		
		assertFalse(cut.hasNext());
	}
	
	/**
	 * Executes a "SELECT ALL" query.
	 * 
	 * @return the {@link ResultSet} as result of the query execution.
	 */
	ResultSet executeQuery() {
		final Query query = QueryFactory.create("SELECT * WHERE {?s ?p ?o}");
		final QueryExecution execution = QueryExecutionFactory.create(query, model);
		return execution.execSelect();		
	}
}