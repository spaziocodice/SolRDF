package org.gazzax.labs.solrdf.handler.search.faceting.rq;

import java.text.ParseException;
import java.util.Date;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.schema.DateField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.DateMathParser;
import org.gazzax.labs.solrdf.handler.search.faceting.RDFacets;
import org.gazzax.labs.solrdf.log.Log;
import org.gazzax.labs.solrdf.log.MessageCatalog;
import org.gazzax.labs.solrdf.log.MessageFactory;
import org.slf4j.LoggerFactory;

/**
 * A {@link RangeEndpointCalculator} for {@link Date} datatype.
 * There is a similar class within {@link SimpleFacets} that unfortunately has a default visibility and 
 * therefore cannot be used within {@link RDFacets} (being this latter in a different package).
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
@SuppressWarnings("deprecation")
public class DateRangeEndpointCalculator extends RangeEndpointCalculator<Date> {
	private final static Log LOGGER = new Log(LoggerFactory.getLogger(DateRangeEndpointCalculator.class));
	
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
			final String message = MessageFactory.createMessage(MessageCatalog._00103_UNABLE_PARSE_DATEMATH_EXPRESSION, gap);
			LOGGER.error(message, exception);
			throw new SolrException(ErrorCode.BAD_REQUEST, message); 
		}
	}
}