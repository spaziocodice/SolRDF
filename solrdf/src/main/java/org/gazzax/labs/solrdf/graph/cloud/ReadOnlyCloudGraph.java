package org.gazzax.labs.solrdf.graph.cloud;

import static org.gazzax.labs.solrdf.F.fq;
import static org.gazzax.labs.solrdf.NTriples.asNt;
import static org.gazzax.labs.solrdf.NTriples.asNtURI;
import static org.gazzax.labs.solrdf.Strings.isNotNullOrEmptyString;

import java.util.Iterator;
import java.util.UUID;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.gazzax.labs.solrdf.Field;
import org.gazzax.labs.solrdf.Strings;
import org.gazzax.labs.solrdf.graph.GraphEventConsumer;
import org.gazzax.labs.solrdf.graph.SolRDFGraph;
import org.gazzax.labs.solrdf.graph.standalone.LocalGraph;
import org.gazzax.labs.solrdf.log.Log;
import org.gazzax.labs.solrdf.log.MessageCatalog;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.AddDeniedException;
import com.hp.hpl.jena.shared.DeleteDeniedException;

/**
 * A read only SolRDF Cloud {@link Graph} implementation.
 * This is a read only graph because changes (i.e. updates and deletes) are executed using 
 * {@link DistributedUpdateProcessor}; that means each node will be responsible to apply local changes using 
 * its own {@link LocalGraph} instance.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public final class ReadOnlyCloudGraph extends SolRDFGraph {
	static final Log LOGGER = new Log(LoggerFactory.getLogger(ReadOnlyCloudGraph.class));
	
	final FieldInjectorRegistry registry = new FieldInjectorRegistry();
	final SolrServer cloud;

	private SolrQuery graphSizeQuery;
	
	/**
	 * Builds a new {@link ReadOnlyCloudGraph} with the given data.
	 * 
	 * @param graphNode the graph name.
	 * @param request the Solr query request.
	 * @param response the Solr query response.
	 * @param qparser the query parser.
	 * @param fetchSize the fetch size that will be used in reads.
	 * @param consumer the Graph event consumer that will be notified on relevant events.
	 */
	ReadOnlyCloudGraph(
		final Node graphNode, 
		final SolrServer cloud, 
		final int fetchSize, 
		final GraphEventConsumer consumer) {
		super(graphNode, consumer, fetchSize);
		this.cloud = cloud;
	}
	
	// TODO: Theoretically the add() could be also done locally. However I think
	// It would result in some unnecessary network roundtrips in case the local 
	// replica is not a leader.
	@Override
	public void performAdd(final Triple triple) {
		final SolrInputDocument document = new SolrInputDocument();
		document.setField(Field.C, graphNodeStringified);
		document.setField(Field.S, asNt(triple.getSubject()));
		document.setField(Field.P, asNtURI(triple.getPredicate()));
		document.setField(Field.ID, UUID.nameUUIDFromBytes(
				new StringBuilder()
					.append(graphNodeStringified)
					.append(triple.getSubject())
					.append(triple.getPredicate())
					.append(triple.getObject())
					.toString().getBytes()).toString());
		
		final String o = asNt(triple.getObject());
		document.setField(Field.O, o);

		final Node object = triple.getObject();
		if (object.isLiteral()) {
			final String language = object.getLiteralLanguage();
			document.setField(Field.LANG, isNotNullOrEmptyString(language) ? language : NULL_LANGUAGE);				

			final RDFDatatype dataType = object.getLiteralDatatype();
			final Object value = object.getLiteralValue();
			registry.get(dataType != null ? dataType.getURI() : null).inject(document, value);
		} else {
			registry.catchAllFieldInjector.inject(document, o);
		}			

		try {
			cloud.add(document);
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00113_NWS_FAILURE, exception);
			throw new AddDeniedException(exception.getMessage(), triple);
		}
	}
	
	@Override
	public void performDelete(final Triple triple) {
		try {
			cloud.deleteByQuery(deleteQuery(triple));
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00113_NWS_FAILURE, exception);
			throw new DeleteDeniedException(exception.getMessage(), triple);
		}	
	}
	
	@Override
	protected int graphBaseSize() {
		try {
			return (int)cloud.query(graphSizeQuery()).getResults().getNumFound();
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00113_NWS_FAILURE, exception);
			throw new SolrException(ErrorCode.SERVER_ERROR, exception);
		}	  
	}
	
	@Override
    public void clear() {
		try {
			cloud.deleteByQuery(fq(Field.C, graphNodeStringified));
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00113_NWS_FAILURE, exception);
			throw new DeleteDeniedException(exception.getMessage());
		}	
	}
	
	@Override
	protected Iterator<Triple> query(final Triple pattern) throws SyntaxError {
		final SolrQuery query = new SolrQuery("*:*");
		query.setSort(Field.ID, ORDER.asc);
	    query.setRows(queryFetchSize);
	    
		final Node s = pattern.getMatchSubject();
		final Node p = pattern.getMatchPredicate();
		final Node o = pattern.getMatchObject();
		
		if (s != null) {
			query.addFilterQuery(fq(Field.S, asNt(s)));
		}
		
		if (p != null) {
			query.addFilterQuery(fq(Field.P, asNtURI(p)));
		}
		
		if (o != null) {
			if (o.isLiteral()) {
				final String language = o.getLiteralLanguage();
				query.addFilterQuery(fq(Field.LANG, (Strings.isNotNullOrEmptyString(language) ? language : NULL_LANGUAGE)));
				
				final String literalValue = o.getLiteralLexicalForm(); 
				final RDFDatatype dataType = o.getLiteralDatatype();
				registry.get(dataType != null ? dataType.getURI() : null).addFilterConstraint(query, literalValue);
			} else {
//				query.addFilterQuery(fq(Field.TEXT_OBJECT, StringEscapeUtils.escapeXml(asNt(o))));		
				query.addFilterQuery(fq(Field.TEXT_OBJECT, asNt(o)));		
			}
		}
		
		query.addFilterQuery(fq(Field.C, graphNodeStringified));			
		
	    return new DeepPagingIterator(cloud, query, consumer);
	}	
	
	/**
	 * Builds a DELETE query.
	 * 
	 * @param triple the triple (maybe a pattern?) that must be deleted.
	 * @return a DELETE query.
	 */
	String deleteQuery(final Triple triple) {
		final StringBuilder builder = new StringBuilder();
		if (triple.getSubject().isConcrete()) {
			builder.append(fq(Field.S, asNt(triple.getSubject()))); 
		}
		
		if (triple.getPredicate().isConcrete()) {
			if (builder.length() != 0) {
				builder.append(" AND ");
			}
			builder.append(fq(Field.P, asNtURI(triple.getPredicate())));
		}
			
		if (triple.getObject().isConcrete()) {
			if (builder.length() != 0) {
				builder.append(" AND ");
			}
			
			final Node o = triple.getObject();
			if (o.isLiteral()) {
				final String language = o.getLiteralLanguage();
				if (Strings.isNotNullOrEmptyString(language)) {
					builder
						.append(fq(Field.LANG, language))
						.append(" AND ");
				}
				
				final String literalValue = o.getLiteralLexicalForm(); 
				final RDFDatatype dataType = o.getLiteralDatatype();
				registry.get(dataType != null ? dataType.getURI() : null).addConstraint(builder, literalValue);
			} else {
				registry.catchAllInjector().addConstraint(builder, asNt(o));
			}
		}
			
		builder.append(" AND ").append(fq(Field.C,graphNodeStringified));
		
		return builder.toString();
	}	
	
	/**
	 * Graph size query command lazy loader.
	 * 
	 * @return the graph size query command.
	 */
	SolrQuery graphSizeQuery() {
		if (graphSizeQuery == null) {
			graphSizeQuery = new SolrQuery("*:*");
			graphSizeQuery.addFilterQuery(fq(Field.C, graphNodeStringified));
			graphSizeQuery.setRows(0);		
		} 
		return graphSizeQuery;
	}
	
	@Override
	protected Log logger() {
		return LOGGER;
	}	
}