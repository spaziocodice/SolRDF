package org.gazzax.labs.solrdf.log;

import org.slf4j.Logger;
import static org.gazzax.labs.solrdf.log.MessageFactory.*;
/**
 * Logger wrapper.
 * A simple SLF4j wrapper that avoids "if" statements within the code.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class Log {
	
	private final Logger logger;

	/**
	 * Builds a new Log wrapper with a given logger.
	 * 
	 * @param logger the {@link Logger} implementation.
	 */
	public Log(final Logger logger) {
		this.logger = logger;
	}

	/**
	 * Logs out the given message with INFO level.
	 * 
	 * @param message the log message.
	 */
	public void info(final String message) {
		logger.info(message);
	}

	/**
	 * Logs out the given message with INFO level.
	 * 
	 * @param message the log message.
	 * @param values values that will replace the message placeholders.
	 */
	public void info(final String message, final Object ... values) {
		logger.info(createMessage(message, values));
	}

	/**
	 * Logs out the given message with DEBUG level.
	 * 
	 * @param message the log message.
	 */
	public void debug(final String message) {
		logger.debug(message);
	}

	/**
	 * Logs out the given message with DEBUG level.
	 * 
	 * @param message the log message.
	 * @param values values that will replace the message placeholders.
	 */
	public void debug(final String message, final Object ... values) {
		if (isDebugEnabled()) {
			logger.debug(createMessage(message, values));
		}
	}

	/**
	 * Logs out the given message with ERROR level.
	 * 
	 * @param message the log message.
	 * @param values values that will replace the message placeholders.
	 */
	public void error(final String message, final Object ... values) {
		logger.error(createMessage(message, values));
	}

	/**
	 * Logs out the given message with ERROR level.
	 * 
	 * @param message the log message.
	 * @param cause the underlying cause.
	 * @param values values that will replace the message placeholders.
	 */
	public void error(final String message, final Throwable cause, final Object ... values) {
		logger.error(createMessage(message, values), cause);
	}

	/**
	 * Logs out the given message with ERROR level.
	 * 
	 * @param message the log message.
	 * @param cause the underlying cause.
	 */
	public void error(final String message, final Throwable cause) {
		logger.error(message, cause);
	}

	/**
	 * Logs out the given message with WARNING level.
	 * 
	 * @param message the log message.
	 */
	public void warning(final String message) {
		logger.warn(message);
	}

	/**
	 * Logs out the given message with WARNING level.
	 * 
	 * @param message the log message.
	 * @param values values that will replace the message placeholders.
	 */
	public void warning(final String message, final Object ... values) {
		logger.warn(createMessage(message, values));
	}

	/**
	 * Returns true if the DEBUG level has been enabled for this logger.
	 * 
	 * @return true if the DEBUG level has been enabled for this logger.
	 */
	public boolean isDebugEnabled() {
		return logger.isDebugEnabled();
	}
}