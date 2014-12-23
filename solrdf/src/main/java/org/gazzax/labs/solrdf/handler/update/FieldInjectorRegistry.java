package org.gazzax.labs.solrdf.handler.update;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.solr.common.SolrInputDocument;
import org.gazzax.labs.solrdf.Field;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDateTime;

/**
 * A registry for datatyped literal objects.
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
	static interface FieldInjector {
		/**
		 * Injects a given value into a document.
		 * 
		 * @param triple the {@link SolrInputDocument} representing a triple.
		 * @param value the value of the object member.
		 */
		public void inject(final SolrInputDocument triple, final Object value);
	}
	
	final FieldInjector booleanFieldInjector = new FieldInjector() {
		@Override
		public void inject(final SolrInputDocument document, final Object value) {
			document.setField(Field.BOOLEAN_OBJECT, value);
		}
	};

	final FieldInjector numericFieldInjector = new FieldInjector() {
		@Override
		public void inject(final SolrInputDocument document, final Object value) {
			document.setField(Field.NUMERIC_OBJECT, value);
		}
	};
	
	final FieldInjector dateTimeFieldInjector = new FieldInjector() {
		@Override
		public void inject(final SolrInputDocument document, Object value) {
			document.setField(Field.DATE_OBJECT, ((XSDDateTime)value).asCalendar().getTime());
		}
	};
	
	final FieldInjector catchAllFieldInjector = new FieldInjector() {
		@Override
		public void inject(final SolrInputDocument document, final Object value) {
			document.setField(Field.TEXT_OBJECT, StringEscapeUtils.escapeXml(String.valueOf(value)));
		}
	};
	
	private final Map<String, FieldInjector> injectors = new HashMap<String, FieldInjector>();
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