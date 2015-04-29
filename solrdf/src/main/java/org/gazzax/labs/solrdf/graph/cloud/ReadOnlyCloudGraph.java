package org.gazzax.labs.solrdf.graph.cloud;

import static org.gazzax.labs.solrdf.NTriples.asNt;
import static org.gazzax.labs.solrdf.NTriples.asNtURI;

import java.util.Iterator;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.gazzax.labs.solrdf.Field;
import org.gazzax.labs.solrdf.Strings;
import org.gazzax.labs.solrdf.graph.GraphEventConsumer;
import org.gazzax.labs.solrdf.graph.standalone.LocalGraph;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.impl.GraphBase;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.NullIterator;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

/**
 * A read only SolRDF Cloud {@link Graph} implementation.
 * This is a read only graph because changes (i.e. updates and deletes) are executed using 
 * {@link DistributedUpdateProcessor}; that means each node will be responsible to apply local changes using 
 * its own {@link LocalGraph} instance.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public final class ReadOnlyCloudGraph extends GraphBase {
	static final int DEFAULT_QUERY_FETCH_SIZE = 1000;
	private FieldInjectorRegistry registry = new FieldInjectorRegistry();
	final QParser qParser;
	
	final Node graphNode;
	final String graphNodeStringified;
	final SolrServer cloud;
	final int queryFetchSize;
	
	final GraphEventConsumer consumer;

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
		final QParser qparser, 
		final int fetchSize, 
		final GraphEventConsumer consumer) {
		this.graphNode = graphNode;
		this.graphNodeStringified = (graphNode != null) ? asNtURI(graphNode) : null;
		this.qParser = qparser;
		this.queryFetchSize = fetchSize;
		this.consumer = consumer;
		this.cloud = cloud;
	}
	
	@Override
	public void performAdd(final Triple triple) {
//		throw new AddDeniedException("Sorry, this is a read-only dataset graph.", triple);
	}
	
	@Override
	public void performDelete(final Triple triple) {
//		throw new AddDeniedException("Sorry, this is a read-only dataset graph.", triple);
	}
	
	@Override
	protected int graphBaseSize() {
		final SolrQuery query = new SolrQuery("*:*");
		if (graphNodeStringified != null) {
			query.addFilterQuery("c:\"" + graphNodeStringified + "\"");
		}
		query.setRequestHandler("/solr-query");
		query.setRows(0);
		try {
			final QueryResponse response = cloud.query(query);
			return (int)response.getResults().getNumFound();
		} catch (final Exception exception) {
			throw new SolrException(ErrorCode.SERVER_ERROR, exception);
		}	  
	}
	
	@Override
    public void clear() {
//		throw new AddDeniedException("Sorry, this is a read-only dataset graph.", triple);
	}
	
	@Override
	public ExtendedIterator<Triple> graphBaseFind(final TripleMatch pattern) {	
		try {
			return WrappedIterator.createNoRemove(query(pattern));
		} catch (final SyntaxError error) {
			return new NullIterator<Triple>();
		}
	}
	
	/**
	 * Executes a query using the given triple pattern.
	 * 
	 * @param pattern the triple pattern
	 * @return an iterator containing matching triples.
	 * @throws SyntaxError in case the query cannot be executed because syntax errors.
	 */
	Iterator<Triple> query(final TripleMatch pattern) throws SyntaxError {
		final SolrQuery query = new SolrQuery("*:*");
		query.setRequestHandler("/solr-query");
	    query.setRows(queryFetchSize);
	    // ??
	    // cmd.setFlags(cmd.getFlags() | SolrIndexSearcher.GET_DOCSET);
	    
		final Node s = pattern.getMatchSubject();
		final Node p = pattern.getMatchPredicate();
		final Node o = pattern.getMatchObject();
		
		if (s != null) {
			query.addFilterQuery(Field.S + ":\"" + asNt(s) + "\"");
		}
		
		if (p != null) {
			query.addFilterQuery(Field.P + ":\"" + asNtURI(p) + "\"");
		}
		
		if (o != null) {
			if (o.isLiteral()) {
				final String language = o.getLiteralLanguage();
				if (Strings.isNotNullOrEmptyString(language)) {
					query.addFilterQuery(Field.LANG + ":" + language);
				}
				
				final String literalValue = o.getLiteralLexicalForm(); 
				final RDFDatatype dataType = o.getLiteralDatatype();
				registry.get(dataType != null ? dataType.getURI() : null).addFilterConstraint(query, literalValue);
			} else {
				query.addFilterQuery(Field.TEXT_OBJECT + ":\"" + StringEscapeUtils.escapeXml(asNt(o)) + "\"");		
			}
		}
		
		if (graphNode != null) {
			query.addFilterQuery(Field.C + ":\"" + asNtURI(graphNode) + "\"");			
		}
		
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
			builder.append(Field.S).append(":\"").append(ClientUtils.escapeQueryChars(asNt(triple.getSubject()))).append("\"");
		}
		
		if (triple.getPredicate().isConcrete()) {
			if (builder.length() != 0) {
				builder.append(" AND ");
			}
			builder.append(Field.P).append(":\"").append(ClientUtils.escapeQueryChars(asNtURI(triple.getPredicate()))).append("\"");
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
						.append(Field.LANG)
						.append(":")
						.append(language)
						.append(" AND ");
				}
				
				final String literalValue = o.getLiteralLexicalForm(); 
				final RDFDatatype dataType = o.getLiteralDatatype();
				registry.get(dataType != null ? dataType.getURI() : null).addConstraint(builder, literalValue);
			} else {
				registry.catchAllInjector().addConstraint(builder, asNt(o));
			}
		}
			
		
		if (graphNode != null) {
			builder.append(" AND ").append(Field.C).append(":\"").append(ClientUtils.escapeQueryChars(graphNodeStringified)).append("\"");
		}
		
		return builder.toString();
	}	
}