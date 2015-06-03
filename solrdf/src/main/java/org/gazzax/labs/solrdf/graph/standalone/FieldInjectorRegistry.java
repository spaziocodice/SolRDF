package org.gazzax.labs.solrdf.graph.standalone;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.gazzax.labs.solrdf.F.fq;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.common.SolrInputDocument;
import org.gazzax.labs.solrdf.Field;
import org.gazzax.labs.solrdf.log.Log;
import org.gazzax.labs.solrdf.log.MessageCatalog;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDateTime;

/**
 * A registry for datatyped literal objects mappings.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
class FieldInjectorRegistry {
	static final Log LOGGER = new Log(LoggerFactory.getLogger(LocalGraph.class));
	
	private ThreadLocal<SimpleDateFormat> isoFormatterCache = new ThreadLocal<SimpleDateFormat>()
	{
		@Override
		protected SimpleDateFormat initialValue()
		{
			final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
			formatter.setLenient(false);
			return formatter;
		}
	};
	
	/**
	 * Command interface.
	 * 
	 * @author Andrea Gazzarini
	 * @since 1.0
	 */
	interface FieldInjector {
		/**
		 * Injects a given value into a document.
		 * 
		 * @param triple the {@link SolrInputDocument} representing a triple.
		 * @param value the value of the object member.
		 */
		void inject(SolrInputDocument triple, Object value);

		/**
		 * Adds to the given list a new constraint query with the given data.
		 * 
		 * @param filters the filter list.
		 * @param value the new constraint value. 
		 */
		void addFilterConstraint(List<Query> filters, String value);
		
		/**
		 * Appends to a given {@link StringBuilder}, and additional AND clause with the given value.
		 * 
		 * @param builder the query builder.
		 * @param value the value of the additional constraint.
		 */
		void addConstraint(StringBuilder builder, String value);
	}
	
	final FieldInjector booleanFieldInjector = new FieldInjector() {
		@Override
		public void inject(final SolrInputDocument document, final Object value) {
			document.setField(Field.BOOLEAN_OBJECT, value);
		}

		@Override
		public void addFilterConstraint(final List<Query> filters, final String value) {
			filters.add(new TermQuery(new Term(Field.BOOLEAN_OBJECT, value.substring(0,1).toUpperCase())));
		}
		
		@Override
		public void addConstraint(final StringBuilder builder, final String value) {
			builder.append(Field.BOOLEAN_OBJECT).append(":").append(value);
		}		
	};

	final FieldInjector numericFieldInjector = new FieldInjector() {
		@Override
		public void inject(final SolrInputDocument document, final Object value) {
			document.setField(Field.NUMERIC_OBJECT, value instanceof BigDecimal ? ((BigDecimal)value).doubleValue() : value);
		}
		
		@Override
		public void addFilterConstraint(final List<Query> filters, final String value) {
			final Double number = Double.valueOf(value);
			filters.add(NumericRangeQuery.newDoubleRange(Field.NUMERIC_OBJECT, number, number, true, true));
		}		

		@Override
		public void addConstraint(final StringBuilder builder, final String value) {
			builder.append(Field.NUMERIC_OBJECT).append(":").append(value);
		}		
	};
	
	final FieldInjector dateTimeFieldInjector = new FieldInjector() {
		@Override
		public void inject(final SolrInputDocument document, final Object value) {
			document.setField(Field.DATE_OBJECT, ((XSDDateTime)value).asCalendar().getTime());
		}
		
		@Override
		public void addFilterConstraint(final List<Query> filters, final String value) {
			try {
				final Long millisecs = isoFormatterCache.get().parse(value).getTime();
				filters.add(NumericRangeQuery.newLongRange(Field.DATE_OBJECT, millisecs, millisecs, true, true));
			} catch (final ParseException exception) {
				LOGGER.error(MessageCatalog._00110_INVALID_DATE_VALUE, exception, value);
				throw new IllegalArgumentException(exception);
			}
		}				

		@Override
		public void addConstraint(final StringBuilder builder, final String value) {
			builder.append(Field.DATE_OBJECT).append(":").append(value);
		}		
	};
	
	final FieldInjector catchAllFieldInjector = new FieldInjector() {
		@Override
		public void inject(final SolrInputDocument document, final Object value) {
			document.setField(Field.TEXT_OBJECT, String.valueOf(value));
		}

		@Override
		public void addFilterConstraint(final List<Query> filters, final String value) {
			final PhraseQuery query = new PhraseQuery();
			query.add(new Term(Field.TEXT_OBJECT, value));
			filters.add(query);
		}		
		
		@Override
		public void addConstraint(final StringBuilder builder, final String value) {
			builder.append(fq(Field.TEXT_OBJECT, value));
		}				
	};
	
	final Map<String, FieldInjector> injectors = new HashMap<String, FieldInjector>();
	{
		injectors.put(XSDDatatype.XSDboolean.getURI(), booleanFieldInjector);		
		
		injectors.put(XSDDatatype.XSDint.getURI(), numericFieldInjector);
		injectors.put(XSDDatatype.XSDinteger.getURI(), numericFieldInjector);
		injectors.put(XSDDatatype.XSDdecimal.getURI(), numericFieldInjector);		
		injectors.put(XSDDatatype.XSDdouble.getURI(), numericFieldInjector);		
		injectors.put(XSDDatatype.XSDlong.getURI(), numericFieldInjector);
		
		injectors.put(XSDDatatype.XSDdate.getURI(), dateTimeFieldInjector);
		injectors.put(XSDDatatype.XSDdateTime.getURI(), dateTimeFieldInjector);	
		
		injectors.put(null, catchAllFieldInjector);	
	}
	
	/**
	 * Returns the {@link FieldInjector} that is in charge to handle the given (datatype) URI.
	 * 
	 * @param uri the datatype URI.
	 * @return the {@link FieldInjector} that is in charge to handle the given (datatype) URI.
	 */
	public FieldInjector get(final String uri) {
		final FieldInjector injector = injectors.get(uri);
		return injector != null ? injector : catchAllFieldInjector;
	}
	
	/**
	 * Returns the {@link FieldInjector} used for storing plain strings.
	 * 
	 * @return the {@link FieldInjector} used for storing plain strings.
	 */
	public FieldInjector catchAllInjector() {
		return catchAllFieldInjector;
	}
}