package org.gazzax.labs.solrdf.response;

import java.util.List;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.engine.binding.Binding;

public class PagedResultSet implements ResultSet {

	final ResultSet resultSet;
	
	final int rows;
	final int start;
	
	/**
	 * Builds a new {@link ResultSet} wrapper.
	 * 
	 * @param resultSet the wrapped resultset.
	 * @param rows the rows that should be effectively part of this resultset.
	 * @param start the start offset within the wrapped resultset.
	 */
	public PagedResultSet(final ResultSet resultSet, final int rows, final int start) {
		this.resultSet = resultSet;
		this.rows = rows;
		this.start = start;
		
		if (start > 0) {
			for (int i = 0; i < start; i++) {
				if (resultSet.hasNext()) {
					// Interessante sarebbe una implementazione di un RS che scorre e basta, 
					// senza creare alcun oggetto
					resultSet.next();
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
		return getRowNumber() < (start + rows) && resultSet.hasNext();
	}

	@Override
	public QuerySolution next() {
		if (getRowNumber() >= start && getRowNumber() < (start + rows)){
			return resultSet.next();
		}
		// theoretically we won't never reach this
		return null;
	}

	@Override
	public QuerySolution nextSolution() {
		return next();
	}

	@Override
	public Binding nextBinding() {
		if (getRowNumber() > start && getRowNumber() < (start + rows)){
			return resultSet.nextBinding();
		}
		return null;
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

}
