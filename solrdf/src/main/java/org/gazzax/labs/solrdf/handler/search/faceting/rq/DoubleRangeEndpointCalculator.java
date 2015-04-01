package org.gazzax.labs.solrdf.handler.search.faceting.rq;

import org.apache.solr.schema.SchemaField;

public class DoubleRangeEndpointCalculator extends RangeEndpointCalculator<Double> {
	/**
	 * Builds a new {@link RangeEndpointCalculator} associated with the given field.
	 * 
	 * @param f the (schema) field.
	 */
	public DoubleRangeEndpointCalculator(final SchemaField f) {
		super(f);
	}

	@Override
    public String format(final Double value) {
		return (value % 1.0 > 0) ? String.valueOf(value) : String.valueOf(value.intValue());
    }
	
    @Override
	public Double getValue(final String rawValue) {
		return Double.valueOf(rawValue);
	}
   
	@Override
	public Double addGap(final Double value, final String gapRawValue) {
		return value + Double.parseDouble(gapRawValue);
	}
  }