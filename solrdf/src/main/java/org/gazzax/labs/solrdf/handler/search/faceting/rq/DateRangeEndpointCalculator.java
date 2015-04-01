package org.gazzax.labs.solrdf.handler.search.faceting.rq;

import java.text.ParseException;
import java.util.Date;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.schema.DateField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.DateMathParser;

@SuppressWarnings("deprecation")
public class DateRangeEndpointCalculator extends RangeEndpointCalculator<Date> {

	public DateRangeEndpointCalculator(final SchemaField f) {
		super(f);
	}
	
	@Override
	public String format(Date value) {
		return ((DateField)field.getType()).toExternal(value);	
	}
	
	@Override
	public Date getValue(String rawval) {
		return ((DateField) field.getType()).parseMath(null, rawval);
	}

	@Override
	public Date addGap(Date value, String gap) {
		final DateMathParser dmp = new DateMathParser();
		dmp.setNow(value);
		try {
			return dmp.parseMath(gap);
		} catch (final ParseException exception) {
			throw new SolrException(ErrorCode.BAD_REQUEST, "Unable to parse date expression " + gap); 
		}
	}
}