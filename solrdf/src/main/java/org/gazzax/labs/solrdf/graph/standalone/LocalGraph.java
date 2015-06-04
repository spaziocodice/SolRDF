package org.gazzax.labs.solrdf.graph.standalone;

import static org.gazzax.labs.solrdf.NTriples.asNt;
import static org.gazzax.labs.solrdf.NTriples.asNtURI;
import static org.gazzax.labs.solrdf.Strings.isNotNullOrEmptyString;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.gazzax.labs.solrdf.Field;
import org.gazzax.labs.solrdf.graph.GraphEventConsumer;
import org.gazzax.labs.solrdf.graph.SolRDFGraph;
import org.gazzax.labs.solrdf.log.Log;
import org.gazzax.labs.solrdf.log.MessageCatalog;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.GraphEvents;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.AddDeniedException;
import com.hp.hpl.jena.shared.DeleteDeniedException;

/**
 * A local SolRDF {@link Graph} implementation.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public final class LocalGraph extends SolRDFGraph {
	
	static final Log LOGGER = new Log(LoggerFactory.getLogger(LocalGraph.class));
		
	static final Map<String, TermQuery> LANGUAGE_TERM_QUERIES = new HashMap<String, TermQuery>();
	
	private SolrIndexSearcher.QueryCommand graphSizeQueryCommand;
	private DeleteUpdateCommand clearCommand;
	
	final UpdateRequestProcessor updateProcessor;
	final AddUpdateCommand updateCommand;
	final SolrQueryRequest request;
	
	final SolrIndexSearcher searcher;
	final QParser qParser;
	
	final TermQuery graphTermQuery;
		
	private FieldInjectorRegistry registry = new FieldInjectorRegistry();
	
	/**
	 * Creates a Read / Write {@link Graph}.
	 * 
	 * @param graphNode the graph node.
	 * @param request the current Solr request.
	 * @param response the current Solr response.
	 * @param qParser the query parser associated with the current request.
	 * @param consumer the Graph event consumer that will be notified on relevant events.
	 * @return a RW {@link Graph} that can be used both for adding and querying data. 
	 */
	public static LocalGraph readableAndWritableGraph(
			final Node graphNode, 
			final SolrQueryRequest request, 
			final SolrQueryResponse response, 
			final QParser qParser,
			final GraphEventConsumer consumer) {
		return new LocalGraph(graphNode, request, response, qParser, DEFAULT_QUERY_FETCH_SIZE, consumer);
	} 

	/**
	 * Creates a Read / Write {@link Graph}.
	 * 
	 * @param graphNode the graph node.
	 * @param request the current Solr request.
	 * @param response the current Solr response.
	 * @param qParser the query parser associated with the current request.
	 * @param fetchSize the read fetch size.
	 * @param consumer the Graph event consumer that will be notified on relevant events.
	 * @return a RW {@link Graph} that can be used both for adding and querying data. 
	 */
	public static LocalGraph readableAndWritableGraph(
			final Node graphNode, 
			final SolrQueryRequest request, 
			final SolrQueryResponse response, 
			final QParser qParser, 
			final int fetchSize,
			final GraphEventConsumer consumer) {
		return new LocalGraph(graphNode, request, response, qParser, fetchSize, consumer);
	}

	/**
	 * Builds a new {@link LocalGraph} with the given data.
	 * 
	 * @param graphNode the graph name.
	 * @param request the Solr query request.
	 * @param response the Solr query response.
	 * @param qparser the query parser.
	 * @param fetchSize the fetch size that will be used in reads.
	 * @param consumer the Graph event consumer that will be notified on relevant events.
	 */
	private LocalGraph(
		final Node graphNode, 
		final SolrQueryRequest request, 
		final SolrQueryResponse response, 
		final QParser qparser, 
		final int fetchSize, 
		final GraphEventConsumer consumer) {
		super(graphNode, consumer, fetchSize);
		this.graphTermQuery = new TermQuery(new Term(Field.C, graphNodeStringified));
		this.request = request;
		this.updateCommand = new AddUpdateCommand(request);
		this.updateProcessor = request.getCore().getUpdateProcessingChain(null).createProcessor(request, response);
		this.searcher = request.getSearcher();
		this.qParser = qparser;
	}
	
	@Override
	public void performAdd(final Triple triple) {
		updateCommand.clear();
		
		final SolrInputDocument document = new SolrInputDocument();
		this.updateCommand.solrDoc = document;
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
			updateProcessor.processAdd(updateCommand);
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00113_NWS_FAILURE, exception);
			throw new AddDeniedException(exception.getMessage(), triple);
		}
	}
	
	@Override
	public void performDelete(final Triple triple) {
		final DeleteUpdateCommand deleteCommand = new DeleteUpdateCommand(request);
		deleteCommand.query = deleteQuery(triple);
		try {
			updateProcessor.processDelete(deleteCommand);
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00113_NWS_FAILURE, exception);
			throw new DeleteDeniedException(exception.getMessage(), triple);
		}		
	}
	
	@Override
	protected int graphBaseSize() {
		final SolrIndexSearcher.QueryResult result = new SolrIndexSearcher.QueryResult();
	    try {
		    return searcher.search(result, graphSizeQueryCommand()).getDocListAndSet().docList.matches();
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00113_NWS_FAILURE, exception);
			throw new SolrException(ErrorCode.SERVER_ERROR, exception);
		}	    
	}
	
	@Override
    public void clear() {
		try {
			updateProcessor.processDelete(clearCommand());
	        getEventManager().notifyEvent(this, GraphEvents.removeAll);
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00113_NWS_FAILURE, exception);
			throw new DeleteDeniedException("Unable to clean this graph " + this);
		}
	}
	
	@Override
	protected Iterator<Triple> query(final Triple pattern) throws SyntaxError {
	    final SolrIndexSearcher.QueryCommand cmd = new SolrIndexSearcher.QueryCommand();
	    final SortSpec sortSpec = qParser != null ? qParser.getSort(true) : QueryParsing.parseSortSpec("id asc", request);
	    cmd.setQuery(new MatchAllDocsQuery());
	    cmd.setSort(sortSpec.getSort());
	    cmd.setLen(queryFetchSize);
	    cmd.setFlags(cmd.getFlags() | SolrIndexSearcher.GET_DOCSET);
	    
	    final List<Query> filters = new ArrayList<Query>();
	    
		final Node s = pattern.getMatchSubject();
		final Node p = pattern.getMatchPredicate();
		final Node o = pattern.getMatchObject();
		
		if (s != null) {
			filters.add(new TermQuery(new Term(Field.S, asNt(s))));
		}
		
		if (p != null) {
			filters.add(new TermQuery(new Term(Field.P, asNtURI(p))));
		}
		
		if (o != null) {
			if (o.isLiteral()) {
				final String language = o.getLiteralLanguage();
				filters.add(isNotNullOrEmptyString(language) ? languageTermQuery(language) : NULL_LANGUAGE_TERM_QUERY);
				
				final String literalValue = o.getLiteralLexicalForm(); 
				final RDFDatatype dataType = o.getLiteralDatatype();
				registry.get(dataType != null ? dataType.getURI() : null).addFilterConstraint(filters, literalValue);
			} else {
				filters.add(new TermQuery(new Term(Field.TEXT_OBJECT, asNt(o))));		
			}
		}
		
		filters.add(graphTermQuery);				
		
		cmd.setFilterList(filters);
	    return new DeepPagingIterator(searcher, cmd, sortSpec, consumer);
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
			builder
				.append(Field.S)
				.append(":\"")
				.append(asNt(triple.getSubject()))
				.append("\"");
		}
		
		if (triple.getPredicate().isConcrete()) {
			if (builder.length() != 0) {
				builder.append(" AND ");
			}
			
			builder
				.append(Field.P)
				.append(":\"")
				.append(asNtURI(triple.getPredicate()))
				.append("\"");
		}
			
		if (triple.getObject().isConcrete()) {
			if (builder.length() != 0) {
				builder.append(" AND ");
			}
			
			final Node o = triple.getObject();
			if (o.isLiteral()) {
				final String language = o.getLiteralLanguage();
				builder
					.append(Field.LANG)
					.append(":")
					.append(isNotNullOrEmptyString(language) ? language : NULL_LANGUAGE)
					.append(" AND ");
				
				final String literalValue = o.getLiteralLexicalForm(); 
				final RDFDatatype dataType = o.getLiteralDatatype();
				registry.get(dataType != null ? dataType.getURI() : null).addConstraint(builder, literalValue);
			} else {
				registry.catchAllInjector().addConstraint(builder, asNt(o));
			}
		}
			
		
		return builder
			.append(" AND ")
			.append(Field.C)
			.append(":\"")
			.append(graphNodeStringified)
			.append("\"")
			.toString();
	}	
	
	/**
	 * Returns a language {@link TermQuery} from the cache.
	 * If the cache doesn't contain a query for a specific language, it 
	 * will be created, cached and returned.
	 * 
	 * @param language the language.
	 * @return a language {@link TermQuery} from the cache.
	 */
	TermQuery languageTermQuery(final String language) {
		TermQuery query = LANGUAGE_TERM_QUERIES.get(language);
		if (query == null) {
			query = new TermQuery(new Term(Field.LANG, language));
			LANGUAGE_TERM_QUERIES.put(language, query);
		}
		return query;
	}
	
	/**
	 * Graph size query command lazy loader.
	 * 
	 * @return the graph size query command.
	 */
	SolrIndexSearcher.QueryCommand graphSizeQueryCommand() {
		if (graphSizeQueryCommand == null) {
			graphSizeQueryCommand = new SolrIndexSearcher.QueryCommand();
			graphSizeQueryCommand.setQuery(new MatchAllDocsQuery());
			graphSizeQueryCommand.setLen(0);
			graphSizeQueryCommand.setFilterList(graphTermQuery);	
		}
		return graphSizeQueryCommand;
	}
	
	/**
	 * Clear graph command lazy loader.
	 * 
	 * @return the clear graph command. 
	 */
	DeleteUpdateCommand clearCommand() {
		if (clearCommand == null) {
			clearCommand = new DeleteUpdateCommand(request);
			clearCommand.query = 
					new StringBuilder(Field.C)
						.append(":\"")
						.append(graphNodeStringified)
						.append("\"")
						.toString();
			clearCommand.commitWithin = 1000;			
		}
		return clearCommand;
	}
	
	@Override
	protected Log logger() {
		return LOGGER;
	}
}