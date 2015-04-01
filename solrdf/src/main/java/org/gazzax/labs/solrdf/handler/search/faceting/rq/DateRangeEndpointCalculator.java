package org.gazzax.labs.solrdf.handler.search.faceting.rq;

import java.text.ParseException;
import java.util.Date;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.schema.DateField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.DateMathParser;

/**
 * A {@link RangeEndpointCalculator} for {@link Date} datatype.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
@SuppressWarnings("deprecation")
public class DateRangeEndpointCalculator extends RangeEndpointCalculator<Date> {

	/**
	 * Builds a new {@link DateRangeEndpointCalculator} associated with the given field.
	 * 
	 * @param f the (schema) field.
	 */
	public DateRangeEndpointCalculator(final SchemaField f) {
		super(f);
	}
	
	@Override
	public String format(final Date value) {
		return ((DateField)field.getType()).toExternal(value);	
	}
	
	@Override
	public Date getValue(final String rawValue) {
		return ((DateField) field.getType()).parseMath(null, rawValue);
	}

	@Override
	public Date addGap(final Date value, final String gap) {
		final DateMathParser dmp = new DateMathParser();
		dmp.setNow(value);
		try {
			return dmp.parseMath(gap);
		} catch (final ParseException exception) {
			throw new SolrException(ErrorCode.BAD_REQUEST, "Unable to parse date expression " + gap); 
		}
	}
}