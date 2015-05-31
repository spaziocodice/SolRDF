package org.gazzax.labs.solrdf.graph.cloud;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.gazzax.labs.solrdf.Field;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;

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
		public void addFilterConstraint(final SolrQuery query, final String value) {
			query.addFilterQuery(Field.DATE_OBJECT + ":" + value);
		}				

		@Override
		public void addConstraint(final StringBuilder builder, final String value) {
			builder.append(Field.DATE_OBJECT).append(":").append(value);
		}		
	};
	
	final FieldInjector catchAllFieldInjector = new FieldInjector() {
		@Override
		public void addFilterConstraint(final SolrQuery query, final String value) {
			query.addFilterQuery(Field.TEXT_OBJECT + ":\"" + ClientUtils.escapeQueryChars(value) + "\"");
		}		
		
		@Override
		public void addConstraint(final StringBuilder builder, final String value) {
			builder.append(Field.TEXT_OBJECT).append(":\"").append(ClientUtils.escapeQueryChars(value) + "\"");
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