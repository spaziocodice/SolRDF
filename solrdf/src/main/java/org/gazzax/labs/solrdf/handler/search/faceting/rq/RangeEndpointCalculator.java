package org.gazzax.labs.solrdf.handler.search.faceting.rq;

import org.apache.solr.request.SimpleFacets;
import org.apache.solr.schema.SchemaField;

/**
 * A range endpoint calculator.
 * If you have a look at Solr code, you will find an identical static inner class within {@link SimpleFacets}.
 * 
 * IMPORTANT: I had to copy and paste (with a bit of changes) that class (and the other two subclasses within 
 * this package) because they are private (default visibility) to {@link SimpleFacets} class.
 * 
 * @author Andrea Gazzarini
 * @see SimpleFacets
 * @param <T> the target datatype of this calculator.
 */
public abstract class RangeEndpointCalculator<T extends Comparable<T>> {

	protected final SchemaField field;
    
	/**
	 * Builds a new {@link RangeEndpointCalculator} with the given field.
	 * 
	 * @param field the schema field.
	 */
	public RangeEndpointCalculator(final SchemaField field) {
      this.field = field;
    }

	/**
	 * Formats a given value in a string form.
	 * 
	 * @param value the input value.
	 * @return the string version of the input value, according with its datatype.
	 */
    public String format(final T value) {
      return value.toString();
    }

    /**
     * Converts a string into a valid value of the datatype managed by this calculator.
     * 
     * @param rawValue the value as a string.
     * @return a valid value of the datatype managed by this calculator.
     */
    public abstract T getValue(String rawValue);

    /**
     * According with the <T> datatype rules, converts and adds a given gap to a given value.
     * 
     * @param value the start value.
     * @param gap the gap to be converted and added.
     * @return a valid value for the <T> datatype which represents "value" incremented by "gap".
     */
    public abstract T addGap(T value, String gap);
  }