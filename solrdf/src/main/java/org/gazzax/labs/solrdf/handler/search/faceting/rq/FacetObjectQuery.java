package org.gazzax.labs.solrdf.handler.search.faceting.rq;

import static org.gazzax.labs.solrdf.Strings.isNullOrEmpty;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.SolrParams;
import org.gazzax.labs.solrdf.Field;

/**
 * A stupid value object for encapsulating a facet query with all related parameters.
 * 
 * Apart from some parameter like alias, hint, all other parameters are described in the Solr Wiki or
 * Reference Guide.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 * @see https://cwiki.apache.org/confluence/display/solr/Faceting
 * @see https://cwiki.apache.org/confluence/display/solr/Faceting#Faceting-RangeFaceting
 */
public class FacetObjectQuery {
	public static String QUERY = FacetParams.FACET + ".object.q";
	public static String QUERY_HINT = QUERY + ".hint";
	public static String QUERY_ALIAS = QUERY + ".alias";
	
	private final int index;
	
	private final SolrParams optionals;
	private final SolrParams requireds;
	
	String fieldName;
	private final String q;
	private final String alias;
	
	/**
	 * Builds a new {@link FacetObjectQuery}.
	 * 
	 * @param q the query.
	 * @param index the query index.
	 * @param alias the query alias.
	 * @param optionals the incoming parameters.
	 * @param requireds the incoming required parameters.
	 */
	private FacetObjectQuery(
			final String q, 
			final int index,
			final String alias,
			final SolrParams optionals,
			final SolrParams required) {
		this.q = q;
		this.index = index;
		this.requireds = required;
		this.optionals = optionals;		
		this.alias = alias != null ? alias : optionals.get(fdqn(QUERY_ALIAS));
	}
	
	/**
	 * Factory method for creating a new anonymous (not indexed) query.
	 * 
	 * @param q the query string.
	 * @param alias the query alias.
	 * @param optionals the incoming parameters.
	 * @param requireds the incoming required parameters.
	 * @return a new anonymous (not indexed) query.
	 */
	public static FacetObjectQuery newAnonymousQuery(
			final String q,
			final String alias, 
			final SolrParams optionals,
			final SolrParams required) {
		return new FacetObjectQuery(q, 0, alias, optionals, required);
	}

	/**
	 * Factory method for creating a new indexed query.
	 * 
	 * @param q the query string.
	 * @param index the index.
	 * @param optionals the incoming parameters.
	 * @param requireds the incoming required parameters.
	 * @return a new anonymous (not indexed) query.
	 */
	public static FacetObjectQuery newQuery(
			final String q,
			final int index,
			final SolrParams optionals,
			final SolrParams required) {
		return new FacetObjectQuery(q, index, null, optionals, required);
	}

	/**
	 * Returns the query string associated with this query object.
	 * 
	 * @return the query string associated with this query object.
	 */
	public String query() {
		return q;
	}

	/**
	 * Returns the alias associated with this query object.
	 * 
	 * @return the alias associated with this query object.
	 */
	public String alias() {
		return alias;
	}

	/**
	 * Returns the target field of this facet query.
	 * 
	 * @return the target field of this facet query.
	 */
	public String fieldName() {
		if (fieldName != null) {
			return fieldName;
		}
		
		return fieldName = 
				"date".equals(optionalString(QUERY_HINT)) 
					? Field.DATE_OBJECT 
					: Field.NUMERIC_OBJECT;		
	}

	/**
	 * Returns a required int parameter value.
	 * 
	 * @param name the parameter name.
	 * @return the value for the requested parameter.
	 * @throws SolrException in case a valid value cannot be found.
	 */
	public int requiredInt(final String name) {
		return Integer.parseInt(requiredString(name));
	}
	
	/**
	 * Returns a required string parameter value.
	 * 
	 * @param name the parameter name.
	 * @return the value for the requested parameter.
	 * @throws SolrException in case a valid value cannot be found.
	 */
	public String requiredString(final String name) {		
		final String result = optionals.get(fdqn(name));
		return isNullOrEmpty(result) ? requireds.get(name) : result;
	}
	
	/**
	 * Returns an optional boolean parameter value.
	 * 
	 * @param name the parameter name.
	 * @return the value for the requested parameter.
	 */
	public boolean optionalBoolean(final String name) {
		return Boolean.parseBoolean(optionalString(name));
	}
	
	/**
	 * Returns the value of the parameter associated with a given name.
	 * 
	 * @param name the parameter name.
	 * @return the value of the parameter associated with a given name, null if it doesn't exist.
	 */
	public String optionalString(final String name) {
		final String result = optionals.get(fdqn(name));
		return isNullOrEmpty(result) ? optionals.get(name) : result;		
	}

	/**
	 * Returns the value of the parameter associated with a given name.
	 * 
	 * @param name the parameter name.
	 * @return the value of the parameter associated with a given name, null if it doesn't exist.
	 */
	public String [] optionalStrings(final String name) {
		final String [] result = optionals.getParams(fdqn(name));
		return result == null || result.length == 0 ? optionals.getParams(name) : result;		
	}

	/**
	 * Returns the scope suffix for this query.
	 * 
	 * @return the scope suffix for this query.
	 */
	String suffix() {
		return isAnonymous() ? "" : "." + index;
	}
	
	/**
	 * Returns the fully qualified name of the given attribute.
	 * In case the query object is anonymous then the result is equal to the input parameter.
	 * Otherwise, a suffix is appended to the attribute name.
	 * 
	 * @param unscopedName the plain attribute name, without any scope.
	 * @return the fully qualified name of the given attribute.
	 */
	String fdqn(final String unscopedName) {
		return new StringBuilder(unscopedName).append(suffix()).toString();
	}
	
	/**
	 * Returns the key identifier associated with this facet range query.
	 * 
	 * @return the key identifier associated with this facet range query.
	 */
	public String key() {
		return alias != null ? alias : q;
	}
	
	/**
	 * Returns true if this {@link FacetObjectQuery} is anonymous.
	 * 
	 * @return true if this {@link FacetObjectQuery} is anonymous.
	 */
	public boolean isAnonymous() {
		return index == 0;
	}
}