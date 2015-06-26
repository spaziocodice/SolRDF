package org.gazzax.labs.solrdf.response;

import static org.apache.solr.common.util.XML.escapeAttributeValue;
import static org.gazzax.labs.solrdf.Strings.isNotNullOrEmptyString;

import java.io.IOException;
import java.io.Writer;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.output.WriterOutputStream;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.response.XMLWriter;
import org.gazzax.labs.solrdf.Names;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.sparql.resultset.XMLOutput;

/**
 * A subclass of {@link XMLWriter} for mixing up Solr and Jena results.
 * Unfortunately something needs to be declared again here because, although it is not final, 
 * I assume the {@link XMLWriter} hasn't been thought with extensibility in mind.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
class HybridXMLWriter extends XMLWriter {
	protected static final char[] XML_PROCESSING_INSTR = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n".toCharArray();
	protected static final char[] RESPONSE_ROOT_ELEMENT_START = ("<response>\n").toCharArray();
	protected static final char[] RESPONSE_ROOT_ELEMENT_END = ("</response>").toCharArray();

	protected static final char[] XML_STYLESHEET = "<?xml-stylesheet type=\"text/xsl\" href=\"".toCharArray();
	protected static final char[] XML_STYLESHEET_END = "\"?>\n".toCharArray();
	protected static final String RESPONSE_HEADER = "responseHeader";

	/**
	 * Builds a new {@link HybridXMLWriter} with the given data.
	 * 
	 * @param writer the output {@link Writer}.
	 * @param request the current {@link SolrQueryRequest}.
	 * @param response the current {@link SolrQueryResponse}.
	 */
	HybridXMLWriter(
			final Writer writer, 
			final SolrQueryRequest request,
			final SolrQueryResponse response) {
		super(writer, request, response);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void writeResponse() throws IOException {
		writer.write(XML_PROCESSING_INSTR);

		final String stylesheet = req.getParams().get("stylesheet");
		if (isNotNullOrEmptyString(stylesheet)) {
			writer.write(XML_STYLESHEET);

			escapeAttributeValue(stylesheet, writer);
			
			writer.write(XML_STYLESHEET_END);
		}

		writer.write(RESPONSE_ROOT_ELEMENT_START);

		final NamedList<?> responseValues = rsp.getValues();
		if (req.getParams().getBool(CommonParams.OMIT_HEADER, false)) {
			responseValues.remove(RESPONSE_HEADER);
		} else {
			((NamedList)responseValues.get(RESPONSE_HEADER)).add(Names.QUERY, responseValues.remove(Names.QUERY).toString());
		}
		
		for (final Entry<String, ?> entry : responseValues) {
			writeValue(entry.getKey(), entry.getValue(), responseValues);			
		}
	
		writer.write(RESPONSE_ROOT_ELEMENT_END);
	}
	
	/**
	 * Writes out a given name / value pair. 
	 * This is similar to {@link XMLWriter#writeVal(String, Object)}. 
	 * This is needed because that similar method is not extensible and cannot be overriden 
	 * (it is called recursively by other methods).
	 * 
	 * @param name the name of the attribute.
	 * @param value the value of the attribute.
	 * @param data the complete set of response values.
	 * @throws IOException in case of I/O failure.
	 */
	public void writeValue(final String name, final Object value, final NamedList<?> data) throws IOException {
		if (value == null) {
			writeNull(name);	
		} else if (value instanceof ResultSet) {
			final int start = req.getParams().getInt(CommonParams.START, 0);
			final int rows = req.getParams().getInt(CommonParams.ROWS, 10);
			writeStartDocumentList("response", start, rows, (Integer) data.remove(Names.NUM_FOUND), 1.0f);
			final XMLOutput outputter = new XMLOutput(false);
			outputter.format(new WriterOutputStream(writer), (ResultSet)value);
			writeEndDocumentList();
		} else if (value instanceof String || value instanceof Query) {
			writeStr(name, value.toString(), false);
		} else if (value instanceof Number) {
			if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
				writeInt(name, value.toString());
			} else if (value instanceof Long) {
				writeLong(name, value.toString());
			} else if (value instanceof Float) {
				writeFloat(name, ((Float) value).floatValue());
			} else if (value instanceof Double) {
				writeDouble(name, ((Double) value).doubleValue());
			} 
		} else if (value instanceof Boolean) {
			writeBool(name, value.toString());
		} else if (value instanceof Date) {
			writeDate(name, (Date) value);
		} else if (value instanceof Map) {
			writeMap(name, (Map<?,?>) value, false, true);
		} else if (value instanceof NamedList) {
			writeNamedList(name, (NamedList<?>) value);
		} else if (value instanceof Iterable) {
			writeArray(name, ((Iterable<?>) value).iterator());
		} else if (value instanceof Object[]) {
			writeArray(name, (Object[]) value);
		} else if (value instanceof Iterator) {
			writeArray(name, (Iterator<?>) value);
		} 
	}
}