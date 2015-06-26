package org.gazzax.labs.solrdf.log;

import static org.mockito.Mockito.*;
import static org.gazzax.labs.solrdf.TestUtility.*;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

/**
 * Test case for {@link Log} class.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class LogTestCase {
	final Object [] randomValues = { randomString(), randomString() };
	
	private Log log;
	private Logger logger;
	
	/**
	 * Setup fixture for this test case.
	 */
	@Before
	public void setUp() {
		logger = mock(Logger.class);
		log = new Log(logger);
	}
	
	@Test
	public void infoWithMessage() {
		final String message = randomString();
		log.info(message);
		verify(logger).info(message);
	}
	
	@Test
	public void infoWithMessageAndPlaceHolders() {
		final String message = randomString();
		final Object [] values = {randomString(), randomString()};
		
		log.info(message, values);
		verify(logger).info(anyString());
	}
	
	@Test
	public void debugWithMessage() {
		final String message = randomString();
		log.debug(message);
		verify(logger).debug(message);
	}	
	
	@Test
	public void debugWithMessageAndPlaceHolders() {
		final String message = randomString();
		final Object [] values = {randomString(), randomString()};
		
		when(logger.isDebugEnabled()).thenReturn(true);
		
		log.debug(message, values);
		verify(logger).isDebugEnabled();
		verify(logger).debug(anyString());
	}	
	
	@Test
	public void debugWithDebugLevelDisabled() {
		final String message = randomString();
		final Object [] values = {randomString(), randomString()};
		
		when(logger.isDebugEnabled()).thenReturn(false);
		
		log.debug(message, values);
		verify(logger).isDebugEnabled();
		verifyNoMoreInteractions(logger);
	}		
	
	@Test
	public void errorWithMessageAndPlaceHolders() {		
		log.error(randomString(), randomValues);
		verify(logger).error(anyString());
	}	
	
	@Test
	public void errorWithMessageThrowableAndPlaceHolders() {		
		final Exception exception = new Exception();
		log.error(randomString(), exception, randomValues);
		verify(logger).error(anyString(), any(Exception.class));
	}	
	
	@Test
	public void errorWithMessageAndThrowable() {		
		final String message = randomString();
		final Exception exception = new Exception();
		
		log.error(message, exception);
		verify(logger).error(message, exception);
	}		
	
	@Test
	public void warningWithMessage() {
		final String message = randomString();
		log.warning(message);
		verify(logger).warn(message);
	}
	
	@Test
	public void warningWithMessageAndPlaceHolders() {
		final String message = randomString();
		
		log.warning(message, randomValues);
		verify(logger).warn(anyString());
	}	
}