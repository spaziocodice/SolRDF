package org.gazzax.labs.solrdf.removeme;

import static org.gazzax.labs.solrdf.NTriples.asNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;
import org.gazzax.labs.solrdf.Field;
import org.gazzax.labs.solrdf.Names;
import org.gazzax.labs.solrdf.graph.standalone.LocalGraph;
import org.gazzax.labs.solrdf.handler.search.algebra.CompositePatternDocSet;
import org.gazzax.labs.solrdf.handler.search.algebra.LeafPatternDocSet;
import org.gazzax.labs.solrdf.handler.search.algebra.PatternDocSet;
import org.gazzax.labs.solrdf.log.Log;
import org.gazzax.labs.solrdf.log.MessageCatalog;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.binding.BindingFactory;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;

public class BgpReducer implements Reducer<OpBGP> {
	private final static Log LOGGER = new Log(LoggerFactory.getLogger(BgpReducer.class));
	
	private List<PatternDocSet> triplePatternDocSets = new ArrayList<PatternDocSet>();
	

	@Override
	public Reducer<OpBGP> apply(final OpFilter filter) {
		// TODO: A quale tripla / triple devo applicare il filtro???
		return null;
	}	
	
	@Override
	public PatternDocSet reduce(final OpBGP op, final ExecutionContext context) {
    	final SolrQueryRequest request = (SolrQueryRequest)context.getContext().get(Names.SOLR_REQUEST_SYM);
 		final SolrIndexSearcher searcher = (SolrIndexSearcher) request.getSearcher();
 		
    	try {
			final Set<String> alreadyCollectedVariables = new HashSet<String>();
			final Iterator<PatternDocSet> iterator = docsets(op.getPattern(), request, (LocalGraph)context.getActiveGraph()).iterator();
			
//			PatternDocSet pivot = getInput() instanceof ExtendedQueryIterator ? ((ExtendedQueryIterator)getInput()).patternDocSet() : iterator.next();
			PatternDocSet pivot = iterator.next();
			while (iterator.hasNext()) {
				final PatternDocSet subsequent = iterator.next();	
				pivot = collectBindings(pivot, subsequent, searcher, alreadyCollectedVariables);
			}
			
			collectBindings(pivot, searcher, alreadyCollectedVariables);
			
			return pivot;
			
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00113_NWS_FAILURE, exception);
			throw new RuntimeException();
		}    	
	}
	
	void add(final Triple pattern, final DocSet docset) {
		triplePatternDocSets.add(new LeafPatternDocSet(docset, pattern, null));
	}
	
	void collectBindings(final PatternDocSet docset, final SolrIndexSearcher searcher, final Set<String> alreadyCollectedVariables) throws IOException {
		if (docset == null) { 
			return;
		}
		
		final Triple pattern = docset.getTriplePattern();
		final DocIterator iterator = docset.iterator();
		while (iterator.hasNext()) { 
			final Document document = searcher.doc(iterator.nextDoc());
			final BindingMap binding = BindingFactory.create(docset.getParentBinding());
			
			collectBinding(pattern.getSubject(), binding, alreadyCollectedVariables, document, Field.S);
			collectBinding(pattern.getPredicate(), binding, alreadyCollectedVariables, document, Field.P);
			collectBinding(pattern.getObject(), binding, alreadyCollectedVariables, document, Field.O);
			
//			if (!binding.isEmpty()) {
//				bindings.add(binding);
//			}
		}
	}
	
	PatternDocSet collectBindings(
			final PatternDocSet first, 
			final PatternDocSet second, 
			final SolrIndexSearcher searcher, 
			final Set<String> alreadyCollectedVariables) throws IOException {
		final CompositePatternDocSet result = new CompositePatternDocSet();
		final DocIterator iterator = first.iterator();
		final Set<String> localCollectedVariables = new HashSet<String>();
		final Triple pattern = first.getTriplePattern();
		
 		while (iterator.hasNext()) {
			final Document document = searcher.doc(iterator.nextDoc());
			final BindingMap binding = BindingFactory.create(first.getParentBinding());
			final BooleanQuery query = new BooleanQuery();
			
			collectBinding(pattern.getSubject(), second.getTriplePattern().getSubject(), binding, alreadyCollectedVariables, localCollectedVariables, document, Field.S, query);
			collectBinding(pattern.getPredicate(), second.getTriplePattern().getPredicate(), binding, alreadyCollectedVariables, localCollectedVariables, document, Field.P, query);
			collectBinding(pattern.getObject(), second.getTriplePattern().getObject(), binding, alreadyCollectedVariables, localCollectedVariables, document, Field.O, query);
			
			// TODO: e se il binding vuoto???
			final DocSet docset = searcher.getDocSet(query, second);
			if (docset.size() > 0) {
				result.union(new LeafPatternDocSet(docset, second.getTriplePattern(), binding));
			}
		}

		alreadyCollectedVariables.addAll(localCollectedVariables);
		return result;		
	}
	
	void collectBinding(
			final Node member, 
			final BindingMap binding,
			final Set<String> alreadyCollectedVariables, 
			final Document document, 
			final String fieldName) {
		if (member.isVariable() && !alreadyCollectedVariables.contains(member.getName())) {
			binding.add(Var.alloc(member), asNode(document.get(fieldName)));
		}
	}	
	
	void collectBinding(
			final Node memberOfTheFirstPattern, 
			final Node memberOfTheSecondPattern, 
			final BindingMap binding,
			final Set<String> globalCollectedVariables, 
			final Set<String> localCollectedVariables, 
			final Document document, 
			final String fieldName, 
			final BooleanQuery query) {
		if (memberOfTheFirstPattern.isVariable()) {
			final String value = document.get(fieldName);
			if (memberOfTheFirstPattern.equals(memberOfTheSecondPattern)) {
				query.add(new TermQuery(new Term(fieldName, value)), Occur.MUST);
			}
			
			if (!globalCollectedVariables.contains(memberOfTheFirstPattern.getName())) {
				binding.add(Var.alloc(memberOfTheFirstPattern), asNode(value));
				localCollectedVariables.add(memberOfTheFirstPattern.getName());
			}
		}
	}		
	
	/**
	 * Returns the list of {@link PatternDocSet} coming from the execution of all triple patterns with the BGP.
	 * 
	 * @param bgp the basic graph pattern.
	 * @param request the current Solr request.
	 * @param graph the active graph.
	 * @return the list of {@link PatternDocSet} coming from the execution of all triple patterns with the BGP.
	 */
	List<PatternDocSet> docsets(final BasicPattern bgp, final SolrQueryRequest request, final LocalGraph graph) {
		try {
			for (final Triple triplePattern : bgp) {
				final DocSet docset = request.getSearcher()
						.getDocSet(
								QParser.getParser(
										graph.luceneQuery(triplePattern), Names.SOLR_QPARSER, request)
								.getQuery());
				
				if (docset.size() == 0) {
					return triplePatternDocSets;
				}
				
				triplePatternDocSets.add(new LeafPatternDocSet(docset,triplePattern));
	        }
			
			Collections.sort(triplePatternDocSets, new Comparator<DocSet>() {
				@Override
				public int compare(final DocSet o1, final DocSet o2) {
					return o1.size() - o2.size();
				}
			});		
			
			return triplePatternDocSets;
		} catch (final IOException exception) {
			LOGGER.error(MessageCatalog._00118_IO_FAILURE, exception);
			return triplePatternDocSets;			
		} catch (final SyntaxError exception) {
			LOGGER.error(MessageCatalog._00119_QUERY_PARSING_FAILURE, exception);
			return triplePatternDocSets;
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00113_NWS_FAILURE, exception);
			return triplePatternDocSets;
		}
	}
}
