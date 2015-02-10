package org.gazzax.labs.solrdf.graph;

import static org.junit.Assert.assertSame;

import java.util.Map.Entry;

import org.gazzax.labs.solrdf.graph.FieldInjectorRegistry.FieldInjector;
import org.junit.Before;
import org.junit.Test;

/**
 * Test case for {@link FieldInjectorRegistry}.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class FieldInjectorRegistryTestCase {
	private FieldInjectorRegistry cut;
	 
	/**
	 * Setup fixture for this test case.
	 */
	@Before
	public void setUp() {
		cut = new FieldInjectorRegistry();
	}
	
	/**
	 * If an injector has been registered for a given datatype then it must be returned.
	 */
	@Test
	public void registeredInjector() {
		for (final Entry<String, FieldInjector> entry : cut.injectors.entrySet()) {
			final String dataTypeURI = entry.getKey();
			final FieldInjector result = cut.get(dataTypeURI);
			assertSame(entry.getValue(), result);
		}		
	}
	
	/**
	 * In case a null datatype is passed as key a catch-all {@link FieldInjector} is returned.
	 */
	@Test
	public void nullDatatype() {
		assertSame(cut.catchAllInjector(), cut.get(null));
	}
	
	/**
	 * In case a datatype hasn't been registered (i.e. unknown) a catch-all {@link FieldInjector} is returned.
	 */
	@Test
	public void unknownRegistryKey() {
		final String unknownKey = String.valueOf(System.currentTimeMillis());
		assertSame(cut.catchAllInjector(), cut.get(unknownKey));
	}	
}