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
import org.apache.solr.request.SimpleFacets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A dedicated {@link FacetComponent} for SolRDF.
 * As you can see this is very identical to its superclass. 
 * The only difference is that it instantiates {@link RDFacets} for gathering facet information, while the superclass
 * instantiates {@link SimpleFacets}.
 * 
 * Unfortunately the instantiation hasn't been relegated in a compose method so in order to use a custom instance, 
 * we need to rewrite the whole {@link #process(ResponseBuilder)} method.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class RDFacetComponent extends FacetComponent {
	public static Logger log = LoggerFactory.getLogger(RDFacetComponent.class);
	public static final String COMPONENT_NAME = "facet";
	private static final String PIVOT_KEY = "facet_pivot";
	
	@Override
	public void process(final ResponseBuilder responseBuilder) throws IOException {
		if (responseBuilder.doFacets) {
			final ModifiableSolrParams params = new ModifiableSolrParams();
			final SolrParams origParams = responseBuilder.req.getParams();
			final Iterator<String> iterator = origParams.getParameterNamesIterator();
			while (iterator.hasNext()) {
				final String paramName = iterator.next();
				// Deduplicate the list with LinkedHashSet, but _only_ for facet
				// params.
				if (paramName.startsWith(FacetParams.FACET) == false) {
					params.add(paramName, origParams.getParams(paramName));
					continue;
				}
				final HashSet<String> deDupe = new LinkedHashSet<>(Arrays.asList(origParams.getParams(paramName)));
				params.add(paramName, deDupe.toArray(new String[deDupe.size()]));
			}

			final RDFacets facetsLogic = new RDFacets(responseBuilder, responseBuilder.getResults().docSet, params);

			final NamedList<Object> counts = facetsLogic.getFacetCounts();
			final String[] pivots = params.getParams(FacetParams.FACET_PIVOT);
			if (pivots != null && pivots.length > 0) {
				final PivotFacetProcessor pivotProcessor = new PivotFacetProcessor(responseBuilder.req, responseBuilder.getResults().docSet, params, responseBuilder);
				final SimpleOrderedMap<List<NamedList<Object>>> v = pivotProcessor.process(pivots);
				if (v != null) {
					counts.add(PIVOT_KEY, v);
				}
			}

			responseBuilder.rsp.add("facet_counts", counts);
		}
	}
}
