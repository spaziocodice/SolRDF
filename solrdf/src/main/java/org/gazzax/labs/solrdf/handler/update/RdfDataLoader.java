package org.gazzax.labs.solrdf.handler.update;

import static org.gazzax.labs.solrdf.NTriples.asNt;
import static org.gazzax.labs.solrdf.NTriples.asNtURI;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.gazzax.labs.solrdf.Field;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

/**
 * Loads an RDF {@link ContentStream} into Solr.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
class RdfDataLoader extends ContentStreamLoader {
	@Override
	public void load(
			final SolrQueryRequest request, 
			final SolrQueryResponse response,
			final ContentStream stream, 
			final UpdateRequestProcessor processor) throws Exception {
		
		final PipedRDFIterator<Triple> iterator = new PipedRDFIterator<Triple>();
		final PipedRDFStream<Triple> inputStream = new PipedTriplesStream(iterator);
		final ExecutorService executor = Executors.newSingleThreadExecutor();
		
		Runnable parser = new Runnable() {
			@Override
			public void run() {
				try {
					RDFDataMgr.parse(
							inputStream, 
							stream.getStream(), 
							RDFLanguages.contentTypeToLang(stream.getContentType()));
				} catch (final IOException exception) {
					exception.printStackTrace();
				}
			}
		};
		
		executor.submit(parser);
		
		while (iterator.hasNext()) {
			final Triple triple = iterator.next();
			final SolrInputDocument document = new SolrInputDocument();
			document.setField(Field.S, asNt(triple.getSubject()));
			document.setField(Field.P, asNtURI(triple.getPredicate()));
			document.setField(Field.O, asNt(triple.getObject()));

			final Node object = triple.getObject();
			if (object.isLiteral()) {
				final RDFDatatype dataType = object.getLiteralDatatype();
				final Object value = object.getLiteral().getLexicalForm();
				document.setField(Field.LANG, object.getLiteralLanguage());				
				
				if (dataType != null) {
					final String uri = dataType.getURI();
					if (XSDDatatype.XSDboolean.getURI().equals(uri)) {
						document.setField(Field.BOOLEAN_OBJECT, value);
					} else if (
							XSDDatatype.XSDint.getURI().equals(uri) ||
							XSDDatatype.XSDinteger.getURI().equals(uri) ||
							XSDDatatype.XSDdecimal.getURI().equals(uri) ||
							XSDDatatype.XSDdouble.getURI().equals(uri) ||
							XSDDatatype.XSDlong.getURI().equals(uri)) {
						document.setField(Field.NUMERIC_OBJECT, value);
					} else if (
							XSDDatatype.XSDdateTime.equals(uri) || 
							XSDDatatype.XSDdate.equals(uri)) {
						document.setField(Field.DATE_OBJECT, value);										
					} else {
						document.setField(Field.TEXT_OBJECT, StringEscapeUtils.escapeXml(String.valueOf(value)));								
					}
				} else {
					document.setField(Field.TEXT_OBJECT, StringEscapeUtils.escapeXml(String.valueOf(value)));			
				}
			} else {
				document.setField(Field.TEXT_OBJECT, asNt(triple.getObject()));			
			}			
			
			final AddUpdateCommand command = new AddUpdateCommand(request);
			command.solrDoc = document;
			processor.processAdd(command);
		}
	}
}