package org.gazzax.labs.solrdf.search.component;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetRewindable;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.core.ResultBinding;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.sun.tools.internal.xjc.generator.bean.ImplStructureStrategy.Result;

/**
 * A simple in-memory {@link ResultSet} decorator that retains (and remember) a page of results from another underlying {@link ResultSet}.
 * Each instance wraps a concrete {@link ResultSet}. It is instantiated with a given offset and size.
 * 
 * The very first time we iterate over that instance, it actually iterates over the wrapped {@link ResultSet}, it returns and caches only those
 * query solutions that fall between the requested range (offset &lt;= index &lt; offset + size).
 * 
 * Once the page results have been collected, a caller can re-iterate again the (cached) results by calling the {@link #reset()} method.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 * @see http://en.wikipedia.org/wiki/Finite-state_machine
 * @see http://en.wikipedia.org/wiki/Decorator_pattern
 */
public final class PagedResultSet implements ResultSetRewindable {
	
	final ResultSet resultSet;
	final int size;
	final int offset;
    
	private List<QuerySolution> rows = new ArrayList<QuerySolution>();
    
	// Since the wrapped resultset cannot be referenced once the corresponding 
	// QueryExecution is closed, we need dedicated references for these members.
	private List<String> resultVars;
    private Model resourceModel;
    
    /**
     * The iterator state of this {@link ResultSet} when the wrapped {@link Result} is iterated for the first time.
     */
    private ResultSetRewindable firstTimeIteration = new ResultSetRewindable() {

    	@Override
		public boolean hasNext() {
			return resultSet.getRowNumber() < (offset + size) && resultSet.hasNext();
		}

		@Override
		public QuerySolution next() {			
			if (resultSet.getRowNumber() >= offset && getRowNumber() < (offset + size)){
				final QuerySolution solution = resultSet.nextSolution();
				rows.add(solution);
				return solution;
			}
			
			throw new IllegalStateException("Invalid iterable state on this ResultSet!");
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public QuerySolution nextSolution() {
			return next();
		}

		@Override
		public Binding nextBinding() {
			return resultSet.nextBinding();
		}

		@Override
		public int getRowNumber() {
			return resultSet.getRowNumber();
		}

		@Override
		public List<String> getResultVars() {
			return resultSet.getResultVars();
		}

		@Override
		public Model getResourceModel() {
			return resultSet.getResourceModel();
		}

		@Override
		public void reset() {
			currentState = new CachedResultSet();
		}

		@Override
		public int size() {
			return rows.size();
		}
	};

	/**
	 * The iterable state of this {@link ResultSet} once the wrapped {@link ResultSet} has been iterated (i.e. the results page has been caught).
	 * 
	 * @author Andrea Gazzarini
	 * @since 1.0
	 */
	private class CachedResultSet implements ResultSetRewindable {
		
		private ListIterator<QuerySolution> iterator = rows.listIterator();

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public QuerySolution nextSolution() {
			return iterator.next();
		}
		
		@Override
		public Binding nextBinding() {
			return ((ResultBinding)iterator.next()).getBinding();
		}
		
		@Override
		public QuerySolution next() {
			return iterator.next();
		}
		
		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}
		
		@Override
		public int getRowNumber() {
			return iterator.previousIndex() + 1;
		}
		
		@Override
		public List<String> getResultVars() {
			return resultVars;
		}
		
		@Override
		public Model getResourceModel() {
			return resourceModel;
		}

		@Override
		public void reset() {
			iterator = rows.listIterator();
		}

		@Override
		public int size() {
			return rows.size();
		}
	};
	
	// The initial iterable state of this ResultSet.
    private ResultSetRewindable currentState = firstTimeIteration;
    
	/**
	 * Builds a new {@link PagedResultSet} decorator on top of a given {@link ResultSet}.
	 * 
	 * @param resultSet the wrapped resultset.
	 * @param size the rows that should be effectively part of this resultset.
	 * @param offset the start offset within the wrapped resultset.
	 */
	PagedResultSet(final ResultSet resultSet, final int size, final int offset) {
		this.resultSet = resultSet;
		this.size = size > 0 ? size : 0;
		this.offset = offset > 0 ? offset : 0;
		
		if (this.resultSet == null) {
			currentState = new CachedResultSet();			
		} else {
			this.resultVars = resultSet.getResultVars();
			this.resourceModel = resultSet.getResourceModel();			
			if (this.size == 0 && this.resultSet.hasNext()) { 
				// Special case: If we asked for 0 rows we still need to do at least one step iteration
				// This is because we need to trigger a request to Solr in order to collect DocSet (e.g. for faceting) 
				resultSet.next();
				currentState = new CachedResultSet();
			} else {
				if (offset > 0) {
					while (resultSet.hasNext() && resultSet.getRowNumber() < offset) {
						resultSet.nextBinding();
					}				
					
					// Sanity check: if offset is greater than the available 
					// rows then the resulting ResultSet must be empty
					if (!resultSet.hasNext()) {
						currentState = new CachedResultSet();					
					}
				}
			}
		}		
	}
	
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasNext() {
		return currentState.hasNext();
	}

	@Override
	public QuerySolution next() {
		return currentState.nextSolution();
	}

	@Override
	public QuerySolution nextSolution() {
		return currentState.nextSolution();
	}

	@Override
	public Binding nextBinding() {
		return currentState.nextBinding();		
	}

	@Override
	public int getRowNumber() {
		return currentState.getRowNumber();
	}

	@Override
	public List<String> getResultVars() {
		return currentState.getResultVars();
	}

	@Override
	public Model getResourceModel() {
		return currentState.getResourceModel();
	}

    @Override
    public void reset() {
    	currentState.reset();
    }

	@Override
	public int size() {
    	return currentState.size();
	}
}