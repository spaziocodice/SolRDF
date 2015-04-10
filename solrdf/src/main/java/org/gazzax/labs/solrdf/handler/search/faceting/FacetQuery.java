package org.gazzax.labs.solrdf.handler.search.faceting;

import static org.gazzax.labs.solrdf.Strings.isNullOrEmpty;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.gazzax.labs.solrdf.handler.search.faceting.rq.FacetRangeQuery;

/**
 * A simple value object encapsulating a facet query.
 * 
 * Apart from some parameter like alias, hint, all other parameters are described in the Solr Wiki or
 * Reference Guide.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 * @see https://cwiki.apache.org/confluence/display/solr/Faceting
 * @see https://cwiki.apache.org/confluence/display/solr/Faceting#Faceting-RangeFaceting 
 */
public abstract class FacetQuery {
	
	protected final int index;
	
	protected final SolrParams optionals;
	protected final SolrParams requireds;
	
	protected String fieldName;
	protected final String q;
	protected final String alias;
	
	/**
	 * Builds a new {@link FacetRangeQuery}.
	 * 
	 * @param q the query.
	 * @param index the query index.
	 * @param alias the query alias.
	 * @param optionals the incoming parameters.
	 * @param requireds the incoming required parameters.
	 */
	protected FacetQuery(
			final String q, 
			final int index,
			final String alias,
			final SolrParams optionals,
			final SolrParams required) {
		this.q = q;
		this.index = index;
		this.requireds = required;
		this.optionals = optionals;		
		this.alias = alias != null ? alias : optionals.get(fdqn(aliasParameterName()));
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
	 * Returns an optional int parameter value.
	 * 
	 * @param name the parameter name.
	 * @param defaultValue the default value that is applied in case the parameter is missing.
	 * @return the value for the requested parameter.
	 */
	public int optionalInt(final String name, final int defaultValue) {
		final Integer result = optionals.getInt(fdqn(name));
		return result != null ? result : optionals.getInt(name, defaultValue);
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
	 * @param defaultValue the default value.
	 * @return the value of the parameter associated with a given name, null if it doesn't exist.
	 */
	public String optionalStringWithDefault(final String name, final String defaultValue) {
		final String result = optionals.get(fdqn(name));
		return isNullOrEmpty(result) ? optionals.get(name, defaultValue) : result;		
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
	 * Returns true if this {@link FacetRangeQuery} is anonymous.
	 * 
	 * @return true if this {@link FacetRangeQuery} is anonymous.
	 */
	public boolean isAnonymous() {
		return index == 0;
	}	
	
	/**
	 * Returns the unscoped name of the alias parameter, for this query.
	 * 
	 * @return the unscoped name of the alias parameter, for this query.
	 */
	protected abstract String aliasParameterName();
	
	/**
	 * Returns the target field of this facet query.
	 * 
	 * @return the target field of this facet query.
	 */
	public abstract String fieldName();
}