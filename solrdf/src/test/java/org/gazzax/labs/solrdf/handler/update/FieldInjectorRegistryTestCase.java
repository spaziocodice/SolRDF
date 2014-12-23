package org.gazzax.labs.solrdf.handler.update;

import static org.junit.Assert.assertSame;

import java.util.Map.Entry;

import org.gazzax.labs.solrdf.handler.update.FieldInjectorRegistry.FieldInjector;
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
		
		assertSame(cut.catchAllInjector(), cut.get(null));
	}
}