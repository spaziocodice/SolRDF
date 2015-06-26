package org.gazzax.labs.solrdf.log;

/**
 * A simple message builder.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class MessageFactory {
	/**
	 * Creates a new message with the given data.
	 * 
	 * @param prototype the message prototype.
	 * @param args the runtime arguments.
	 * @return a new message.
	 */
	public static final String createMessage(final String prototype, final Object... args) {
		return String.format(prototype, args);
	}
}
