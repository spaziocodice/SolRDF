package org.gazzax.labs.solrdf.handler.search.faceting.rq;

import org.apache.solr.schema.SchemaField;

public abstract class RangeEndpointCalculator<T extends Comparable<T>> {

	protected final SchemaField field;
    
	public RangeEndpointCalculator(final SchemaField field) {
      this.field = field;
    }

    /**
     * Formats a Range endpoint for use as a range label name in the response.
     * Default Impl just uses toString()
     */
    public String format(final T value) {
      return value.toString();
    }

    /**
     * Parses a String param into an Range endpoint value throwing 
     * a useful exception if not possible
     */
    public abstract T getValue(final String rawval);

    /**
     * Adds the String gap param to a low Range endpoint value to determine 
     * the corrisponding high Range endpoint value, throwing 
     * a useful exception if not possible.
     */
    public abstract T addGap(T value, String gap);
  }