package org.gazzax.labs.solrdf.handler.search.faceting;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.FacetComponent;
import org.apache.solr.handler.component.PivotFacetProcessor;
import org.apache.solr.handler.component.ResponseBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RDFacetComponent extends FacetComponent {
	public static Logger log = LoggerFactory.getLogger(RDFacetComponent.class);
	public static final String COMPONENT_NAME = "facet";
	private static final String PIVOT_KEY = "facet_pivot";

	public static void main(String[] args) {
		System.out.println(5007 >> 6);
	}
	
	@Override
	public void process(ResponseBuilder rb) throws IOException {

		// SolrParams params = rb.req.getParams();
		if (rb.doFacets) {
			final ModifiableSolrParams params = new ModifiableSolrParams();
			SolrParams origParams = rb.req.getParams();
			Iterator<String> iter = origParams.getParameterNamesIterator();
			while (iter.hasNext()) {
				String paramName = iter.next();
				// Deduplicate the list with LinkedHashSet, but _only_ for facet
				// params.
				if (paramName.startsWith(FacetParams.FACET) == false) {
					params.add(paramName, origParams.getParams(paramName));
					continue;
				}
				HashSet<String> deDupe = new LinkedHashSet<>(
						Arrays.asList(origParams.getParams(paramName)));
				params.add(paramName, deDupe.toArray(new String[deDupe.size()]));
			}

			final RDFacets f = new RDFacets(rb, rb.getResults().docSet, params);

			NamedList<Object> counts = f.getFacetCounts();
			String[] pivots = params.getParams(FacetParams.FACET_PIVOT);
			if (pivots != null && pivots.length > 0) {
				final PivotFacetProcessor pivotProcessor = new PivotFacetProcessor(rb.req, rb.getResults().docSet, params, rb);
				final SimpleOrderedMap<List<NamedList<Object>>> v = pivotProcessor.process(pivots);
				if (v != null) {
					counts.add(PIVOT_KEY, v);
				}
			}

			rb.rsp.add("facet_counts", counts);
		}
	}
}
