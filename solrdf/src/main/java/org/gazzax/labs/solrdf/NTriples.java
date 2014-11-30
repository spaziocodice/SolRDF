package org.gazzax.labs.solrdf;

import static org.gazzax.labs.solrdf.Strings.isNotNullOrEmptyString;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.rdf.model.AnonId;

/**
 * Booch utility for parsing RDF resources.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class NTriples {
	private static final String START_URI_CHAR = "<";
	private static final String END_URI_CHAR = ">";
	private static final String START_BNODE_CHARS = "_:";
	private static final String START_LITERAL_CHAR = "\"";
	private static final String LANGUAGE_MARKER = "@";
	private static final String DATATYPE_MARKER = "^^";
	
	/**
	 * Returns true if the given NT value denotes a URI.
	 * 
	 * @param nt the value to be checked.
	 * @return true if the given NT value denotes a URI.
	 */
	static boolean isURI(final String nt) {
		return nt != null && nt.startsWith(START_URI_CHAR) && nt.endsWith(END_URI_CHAR);
	}
	
	/**
	 * Returns true if the given NT value denotes a blank node.
	 * 
	 * @param nt the value to be checked.
	 * @return true if the given NT value denotes a blank node.
	 */
	static boolean isBlankNode(final String nt) {
		return nt != null && nt.startsWith(START_BNODE_CHARS);
	}
	
	/**
	 * Returns a {@link Node} representation of a given resource.
	 * 
	 * @param nt the resource (as string in NT format).
	 * @return a {@link Node} representation of a given resource.
	 */
	public static Node asNode(final String nt) {
		if (isURI(nt)) {
			return internalAsURI(nt);
		} else if (isBlankNode(nt)) {
			return internalAsBlankNode(nt);
		} 
		return asLiteral(nt);
	}

	/**
	 * Returns a {@link Node} representation of a given URI or blank node.
	 * 
	 * @param nt the resource (as string in NT format).
	 * @return a {@link Node} representation of a given URI or blank node.
	 */	
	public static Node asURIorBlankNode(final String nt) {
		return (isURI(nt)) ? internalAsURI(nt) : internalAsBlankNode(nt);
	}

	/**
	 * Returns a {@link Node} representation of a given URI.
	 * 
	 * @param nt the resource (as string in NT format).
	 * @return a {@link Node} representation of a given URI.
	 */	
	public static Node asURI(final String nt) {
		if (isURI(nt)) {
			return internalAsURI(nt);
		} 
		throw new IllegalArgumentException(nt);
	}

	/**
	 * Parses the given input as URI.
	 * 
	 * @param uriAsString the URI string value.
	 * @return the {@link Node} URI representation of the given value.
	 */
	private static Node internalAsURI(final String uriAsString) {
		final String uri = unescape(uriAsString.substring(1, uriAsString.length() - 1));
		return NodeFactory.createURI(uri);		
	}
	
	/**
	 * Parses the given input as a blank node.
	 * 
	 * @param blankNodeAsString the blank node string value.
	 * @return the {@link Node} Blank node representation of the given value.
	 */
	private static Node internalAsBlankNode(final String blankNodeAsString) {
		return NodeFactory.createAnon(AnonId.create(blankNodeAsString.substring(2)));	
	}
	
	/**
	 * Returns a {@link Node} representation of a blank node.
	 * 
	 * @param blankNodeAsString the resource (as string in NT format).
	 * @return a {@link Node} representation of a blank node
	 */		
	public static Node asBlankNode(final String blankNodeAsString) {
		if (isBlankNode(blankNodeAsString)) {
			return internalAsBlankNode(blankNodeAsString);
		}
		throw new IllegalArgumentException(blankNodeAsString);
	}
	
	/**
	 * Returns a {@link Node} representation of a literal.
	 * 
	 * @param literal the resource (as string in NT format).
	 * @return a {@link Node} representation of a literal.
	 */		
	public static Node asLiteral(final String literal) {
		if (literal.startsWith(START_LITERAL_CHAR)) {
			int endIndexOfValue = endIndexOfValue(literal);

			if (endIndexOfValue != -1) {
				final String literalValue = unescape(literal.substring(1, endIndexOfValue));

				final int startIndexOfLanguage = literal.indexOf(LANGUAGE_MARKER, endIndexOfValue);
				final int startIndexOfDatatype = literal.indexOf(DATATYPE_MARKER, endIndexOfValue);

				if (startIndexOfLanguage != -1) {
					return NodeFactory.createLiteral(
							literalValue, 
							literal.substring(startIndexOfLanguage + LANGUAGE_MARKER.length()), 
							null);
				} else if (startIndexOfDatatype != -1) {
					return NodeFactory.createLiteral(
							literalValue, 
							null, 
							NodeFactory.getType(literal.substring(startIndexOfDatatype + DATATYPE_MARKER.length())));
				} else {
					return NodeFactory.createLiteral(literalValue);
				}
			}
		}

		throw new IllegalArgumentException(literal);
	}

	/**
	 * Returns the end index of a literal value.
	 * 
	 * @param literalValue the literal value.
	 * @return the end index of a literal value, -1 if that cannot be determined.
	 */
	private static int endIndexOfValue(final String literalValue) {
		boolean previousWasBackslash = false;

		for (int i = 1; i < literalValue.length(); i++) {
			char c = literalValue.charAt(i);

			if (c == '"' && !previousWasBackslash) {
				return i;
			} else if (c == '\\' && !previousWasBackslash) {
				previousWasBackslash = true;
			} else if (previousWasBackslash) {
				previousWasBackslash = false;
			}
		}

		return -1;
	}

	/**
	 * Returns a {@link String} representation of the given {@link Node}.
	 * 
	 * @param node the node.
	 * @return a {@link String} representation of the given {@link Node}.
	 */
	public static String asNt(final Node node) {
		if (node.isURI()) {
			return asNtURI(node);
		} else if (node.isBlank()) {
			return asNtBlankNode(node);		
		} else if (node.isLiteral()) {
			return asNtLiteral(node);
		}
		throw new IllegalArgumentException(node.getClass().getName());
	}

	/**
	 * Returns a {@link String} representation of the given URI.
	 * 
	 * @param uri the URI node.
	 * @return a {@link String} representation of the given URI.
	 */
	public static String asNtURI(final Node uri) {
		final StringBuilder buffer = new StringBuilder("<");
		escapeAndAppend(uri.getURI(), buffer);
		return buffer.append(">").toString();
	}
	
	/**
	 * Returns a {@link String} representation of the given blank node.
	 * 
	 * @param blankNode the blank node.
	 * @return a {@link String} representation of the given blank node.
	 */
	public static String asNtBlankNode(final Node blankNode) {
		return new StringBuilder("_:")
			.append(blankNode.getBlankNodeLabel())
			.toString();
	}
	
	/**
	 * Returns a {@link String} representation of the given literal.
	 * 
	 * @param literal the literal node.
	 * @return a {@link String} representation of the given literal.
	 */
	public static String asNtLiteral(final Node literal) {
		final StringBuilder buffer = new StringBuilder("\"");
		escapeAndAppend(String.valueOf(literal.getLiteral().getLexicalForm()), buffer);
		buffer.append("\"");
		final String language = literal.getLiteralLanguage();
		if (isNotNullOrEmptyString(language)) {
			buffer.append("@").append(language);
		}
		
		final String datatypeURI = literal.getLiteralDatatypeURI();
		if (datatypeURI != null) {
			buffer.append("^^");
			escapeAndAppend(datatypeURI, buffer);
		}
		return buffer.toString();
	}

	/**
	 * Escapes the given value by appending the result in the given buffer.
	 * 
	 * @param value the value to be escaped.
	 * @param buffer the accumulator char buffer.
	 */
	private static void escapeAndAppend(final String value, final StringBuilder buffer) {
		final int labelLength = value.length();

		for (int i = 0; i < labelLength; i++) {
			char c = value.charAt(i);
			int cInt = c;
			switch(c) {
			case '\\':
				buffer.append("\\\\");
				break;
			case '\"' : 
				buffer.append("\\\"");
				break;
			case '\n':
				buffer.append("\\n");
				break;
			case '\r':
				buffer.append("\\r");
				break;
			case '\t':
				buffer.append("\\t");
				break;
			default:
				if (cInt >= 0x0 && cInt <= 0x8 || cInt == 0xB || cInt == 0xC || cInt >= 0xE && cInt <= 0x1F
				|| cInt >= 0x7F && cInt <= 0xFFFF) {
					buffer.append("\\u");
					buffer.append(hex(cInt, 4));
				} else if (cInt >= 0x10000 && cInt <= 0x10FFFF) {
					buffer.append("\\U");
					buffer.append(hex(cInt, 8));
				} else {
					buffer.append(c);
				}
			} 
		}
	}
	
	/**
	 * Unescapes a given value.
	 * 
	 * @param value the value to be unescaped.
	 * @return the unescaped value.
	 */
	public static String unescape(final String value) {
		int indexOfBackSlash = value.indexOf('\\');

		if (indexOfBackSlash == -1) {
			return value;
		}

		int startIndexOfEscapedSequence = 0;
		final int valueLength = value.length();
		final StringBuilder builder = new StringBuilder(valueLength);

		while (indexOfBackSlash != -1) {
			builder.append(value.substring(startIndexOfEscapedSequence, indexOfBackSlash));

			if (indexOfBackSlash + 1 >= valueLength) {
				throw new IllegalArgumentException(value);
			}

			char c = value.charAt(indexOfBackSlash + 1);
			switch(c) {
			case 't' :
				builder.append('\t');
				startIndexOfEscapedSequence = indexOfBackSlash + 2;
				break;
			case 'b' :
				builder.append('\b');
				startIndexOfEscapedSequence = indexOfBackSlash + 2;
				break;
			case 'n' :
				builder.append('\n');
				startIndexOfEscapedSequence = indexOfBackSlash + 2;
				break;
			case 'r' :
				builder.append('\r');
				startIndexOfEscapedSequence = indexOfBackSlash + 2;
				break;
			case 'f':
				builder.append('\f');
				startIndexOfEscapedSequence = indexOfBackSlash + 2;
				break;
			case '"' : 
				builder.append('"');
				startIndexOfEscapedSequence = indexOfBackSlash + 2;
				break;
			case '\'' :
				builder.append('\'');
				startIndexOfEscapedSequence = indexOfBackSlash + 2;
				break;
			case '\\' : 
				builder.append('\\');
				startIndexOfEscapedSequence = indexOfBackSlash + 2;
				break;
			case 'u' : 
				if (indexOfBackSlash + 5 >= valueLength) {
					throw new IllegalArgumentException(value);
				}
				
				final String hex5 = value.substring(indexOfBackSlash + 2, indexOfBackSlash + 6);

				try {
					c = (char)Integer.parseInt(hex5, 16);
					builder.append(c);
					startIndexOfEscapedSequence = indexOfBackSlash + 6;
				} catch (final NumberFormatException e) {
					throw new IllegalArgumentException("Bad unicode escape sequence '\\u" + hex5 + "' in: " + value);
				}
				break;
			case 'U' :
				if (indexOfBackSlash + 9 >= valueLength) {
					throw new IllegalArgumentException(value);
				}
				
				final String hex9 = value.substring(indexOfBackSlash + 2, indexOfBackSlash + 10);

				try {
					c = (char)Integer.parseInt(hex9, 16);
					builder.append(c);
					startIndexOfEscapedSequence = indexOfBackSlash + 10;
				} catch (final NumberFormatException exception) {
					throw new IllegalArgumentException("Bad unicode escape sequence '\\U" + hex9 + "' in: " + value);
				}
				break;
			default:
				throw new IllegalArgumentException("Unescaped backslash found in: " + value);
			}

			indexOfBackSlash = value.indexOf('\\', startIndexOfEscapedSequence);
		}

		builder.append(value.substring(startIndexOfEscapedSequence));

		return builder.toString();
	}

	/**
	 * .
	 * @param decimal
	 * @param length
	 * @return
	 */
	public static String hex(final int decimal, final int length) {
		final StringBuilder builder = new StringBuilder(length);
		final String hex = Integer.toHexString(decimal).toUpperCase();

		final int bound = length - hex.length();
		for (int i = 0; i < bound; i++) {
			builder.append('0');
		}

		return builder.append(hex).toString();
	}
}