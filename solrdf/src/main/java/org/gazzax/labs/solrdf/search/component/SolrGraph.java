package org.gazzax.labs.solrdf.search.component;

import java.util.ArrayList;

import org.apache.solr.search.SolrIndexSearcher;
import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;
import org.gazzax.labs.jena.nosql.fwk.ds.GraphDAO;
import org.gazzax.labs.jena.nosql.fwk.factory.StorageLayerFactory;
import org.gazzax.labs.jena.nosql.fwk.log.Log;
import org.gazzax.labs.jena.nosql.fwk.log.MessageCatalog;
import org.gazzax.labs.jena.nosql.fwk.log.MessageFactory;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.GraphEvents;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.impl.GraphBase;
import com.hp.hpl.jena.shared.AddDeniedException;
import com.hp.hpl.jena.shared.DeleteDeniedException;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

/**
 * SOLR Graph implementation.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SolrGraph extends GraphBase {
	private static final Log LOGGER = new Log(LoggerFactory.getLogger(SolrGraph.class));
	private static final ExtendedIterator<Triple> EMPTY_TRIPLES_ITERATOR = WrappedIterator.createNoRemove(new ArrayList<Triple>(0).iterator());

	private final GraphDAO<Triple, TripleMatch> dao;
	
	private SolrIndexSearcher searcher;
				
	/**
	 * Builds a new unnamed graph with the given factory.
	 * 
	 * @param factory the storage layer factory.
	 */
	public SolrGraph(SolrIndexSearcher searcher) {
		this.dao = new SolrGraphDAO(searcher);
	}

	/**
	 * Builds a new named graph with the given data.
	 * 
	 * @param name the graph name.
	 * @param factory the storage layer factory.
	 */	
	public SolrGraph(final Node name, final SolrIndexSearcher searcher) {
		this.dao = name != null ? new SolrGraphDAO(searcher, name) : new SolrGraphDAO(searcher);
	}
	
	@Override
	public void performAdd(final Triple triple) {
		try {
			dao.insertTriple(triple);
		} catch (final StorageLayerException exception) {
			final String message = MessageFactory.createMessage(MessageCatalog._00101_UNABLE_TO_ADD_TRIPLE, triple);
			LOGGER.error(message, exception);
			throw new AddDeniedException(message, triple);
		}
	}
	
	@Override
	public void performDelete(final Triple triple) {
		try {
			if (triple.isConcrete()) {
				dao.deleteTriple(triple);
			} else if (!triple.getSubject().isConcrete() &&  !triple.getPredicate().isConcrete() &&  !triple.getObject().isConcrete()) {
				clear();
			} else {
				dao.deleteTriple(triple);
			}	
			dao.executePendingMutations();
		} catch (final StorageLayerException exception) {
			final String message = MessageFactory.createMessage(MessageCatalog._00100_UNABLE_TO_DELETE_TRIPLE, triple);
			LOGGER.error(message, exception);
			throw new DeleteDeniedException(message, triple);
		}
	}
	
	@Override
	protected int graphBaseSize() {
		try {
			return (int) dao.countTriples();
		} catch (StorageLayerException exception) {
			LOGGER.error(MessageCatalog._00010_DATA_ACCESS_LAYER_FAILURE, exception);		
			throw new RuntimeException(exception);
		}
	}
	
	@Override
    public void clear() {
	    dao.clear();
        getEventManager().notifyEvent(this, GraphEvents.removeAll);
	}
	
	@Override
	public ExtendedIterator<Triple> graphBaseFind(final TripleMatch pattern) {
		return EMPTY_TRIPLES_ITERATOR;
//		try  {
//			return WrappedIterator.createNoRemove(dao.query(pattern));
//		} catch (StorageLayerException exception) {
//			LOGGER.error(MessageCatalog._00010_DATA_ACCESS_LAYER_FAILURE, exception);
//			return EMPTY_TRIPLES_ITERATOR;
//		}
	}
}