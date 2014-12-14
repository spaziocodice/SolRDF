package org.gazzax.labs.solrdf.search.component;

import java.io.IOException;

import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortSpec;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.GraphEvents;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.impl.GraphBase;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

/**
 * SolRDF {@link Graph} implementation.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SolRDFGraph extends GraphBase {
	private final SolRDFGraphDAO dao;
				
	/**
	 * Builds a new unnamed graph with the given factory.
	 * 
	 * @param factory the storage layer factory.
	 */
	public SolRDFGraph(SolrIndexSearcher searcher, final SortSpec sort) {
		this.dao = new SolRDFGraphDAO(searcher, sort);
	}

	/**
	 * Builds a new named graph with the given data.
	 * 
	 * @param name the graph name.
	 * @param factory the storage layer factory.
	 */	
	public SolRDFGraph(final Node name, final SolrIndexSearcher searcher, final SortSpec sort) {
		this.dao = name != null ? new SolRDFGraphDAO(searcher, name, sort) : new SolRDFGraphDAO(searcher, sort);
	}
	
	@Override
	public void performAdd(final Triple triple) {
		// TODO
	}
	
	@Override
	public void performDelete(final Triple triple) {
		// TODO
	}
	
	@Override
	protected int graphBaseSize() {
		try {
			return (int) dao.countTriples();
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
	}
	
	@Override
    public void clear() {
	    dao.clear();
        getEventManager().notifyEvent(this, GraphEvents.removeAll);
	}
	
	@Override
	public ExtendedIterator<Triple> graphBaseFind(final TripleMatch pattern) {		
		return WrappedIterator.createNoRemove(dao.query(pattern));
	}
}