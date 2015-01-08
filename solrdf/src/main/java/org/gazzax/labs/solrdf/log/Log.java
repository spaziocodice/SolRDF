package org.gazzax.labs.solrdf.log;

import org.slf4j.Logger;

/**
 * Logger wrapper.
 * A simple SLF4j wrapper that avoids "if" statements within the code.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class Log {
	
	private final Logger _logger;

	/**
	 * Builds a new Log wrapper with a given logger.
	 * 
	 * @param logger the {@link Logger} implementation.
	 */
	public Log(final Logger logger) {
		_logger = logger;
	}

	/**
	 * Logs out the given message with INFO level.
	 * 
	 * @param message the log message.
	 */
	public void info(final String message) {
		_logger.info(message);
	}

	/**
	 * Logs out the given message with INFO level.
	 * 
	 * @param message the log message.
	 * @param values values that will replace the message placeholders.
	 */
	public void info(final String message, final Object ... values) {
		_logger.info(String.format(message, values));
	}

	/**
	 * Logs out the given message with DEBUG level.
	 * 
	 * @param message the log message.
	 */
	public void debug(final String message) {
		_logger.debug(message);
	}

	/**
	 * Logs out the given message with DEBUG level.
	 * 
	 * @param message the log message.
	 * @param values values that will replace the message placeholders.
	 */
	public void debug(final String message, final Object ... values) {
		if (_logger.isDebugEnabled()) {
			_logger.debug(String.format(message, values));
		}
	}

	/**
	 * Logs out the given message with ERROR level.
	 * 
	 * @param message the log message.
	 * @param values values that will replace the message placeholders.
	 */
	public void error(final String message, final Object ... values) {
		_logger.error(String.format(message, values));
	}

	/**
	 * Logs out the given message with ERROR level.
	 * 
	 * @param message the log message.
	 * @param cause the underlying cause.
	 * @param values values that will replace the message placeholders.
	 */
	public void error(final String message, final Throwable cause, final Object ... values) {
		_logger.error(String.format(message, values));
	}

	/**
	 * Logs out the given message with ERROR level.
	 * 
	 * @param message the log message.
	 * @param cause the underlying cause.
	 */
	public void error(final String message, final Throwable cause) {
		_logger.error(message, cause);
	}

	/**
	 * Logs out the given message with WARNING level.
	 * 
	 * @param message the log message.
	 */
	public void warning(final String message) {
		_logger.warn(message);
	}

	/**
	 * Logs out the given message with WARNING level.
	 * 
	 * @param message the log message.
	 * @param values values that will replace the message placeholders.
	 */
	public void warning(final String message, final Object ... values) {
		_logger.warn(String.format(message, values));
	}

	/**
	 * Returns true if the DEBUG level has been enabled for this logger.
	 * 
	 * @return true if the DEBUG level has been enabled for this logger.
	 */
	public boolean isDebugEnabled() {
		return _logger.isDebugEnabled();
	}
}