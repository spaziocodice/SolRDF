package org.gazzax.labs.solrdf.graph.cloud;

import static org.gazzax.labs.solrdf.F.fq;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrInputDocument;
import org.gazzax.labs.solrdf.Field;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDateTime;

/**
 * A registry for datatyped literal objects mappings.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
class FieldInjectorRegistry {
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
		void addFilterConstraint(SolrQuery query, String value);
		
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
		public void addFilterConstraint(final SolrQuery query, final String value) {
			query.addFilterQuery(Field.BOOLEAN_OBJECT + ":" + value);
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
		public void addFilterConstraint(final SolrQuery query, final String value) {
			query.addFilterQuery(Field.NUMERIC_OBJECT + ":" + value);
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
		public void addFilterConstraint(final SolrQuery query, final String value) {
			query.addFilterQuery(fq(Field.DATE_OBJECT, value));
		}				

		@Override
		public void addConstraint(final StringBuilder builder, final String value) {
			builder.append(fq(Field.DATE_OBJECT, value));
		}		
	};
	
	final FieldInjector catchAllFieldInjector = new FieldInjector() {
		@Override
		public void inject(final SolrInputDocument document, final Object value) {
			document.setField(Field.TEXT_OBJECT, String.valueOf(value));
		}
		
		@Override
		public void addFilterConstraint(final SolrQuery query, final String value) {
			query.addFilterQuery(fq(Field.TEXT_OBJECT, value));
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