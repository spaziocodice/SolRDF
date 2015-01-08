<!-- 
     filename: ex402.xsl 
     Convert XML SPARQL query results to a DITA Concept document.
-->
<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:s="http://www.w3.org/2005/sparql-results#"
                exclude-result-prefixes="s">

  <xsl:output doctype-public="-//OASIS//DTD DITA Concept//EN" 
              doctype-system="C:\usr\local\dita\DITA-OT1.5\dtd\technicalContent\dtd\concept.dtd"/>

  <xsl:template match="s:sparql">
    <concept id="id1">
      <title>Wind Power Companies</title>
      <conbody>
        <table>
          <tgroup cols="{count(s:head/s:variable)}">
            <xsl:apply-templates/>
          </tgroup>
        </table>
      </conbody>
    </concept>
  </xsl:template>

  <xsl:template match="s:head">
    <thead>
      <row>
        <xsl:apply-templates/>
      </row>
    </thead>
  </xsl:template>

  <xsl:template match="s:variable">
    <entry><xsl:value-of select="@name"/></entry>
  </xsl:template>

  <xsl:template match="s:results">
    <tbody><xsl:apply-templates/></tbody>
  </xsl:template>

  <xsl:template match="s:result">
    <row><xsl:apply-templates/></row>
  </xsl:template>

  <xsl:template match="s:binding">
    <entry><xsl:apply-templates/></entry>
  </xsl:template>

  <xsl:template match="s:literal">
    <xsl:apply-templates/>
  </xsl:template>

  <xsl:template match="s:uri">
    <xref format="html" href="{.}">
      <xsl:apply-templates/>
    </xref>
  </xsl:template>

</xsl:stylesheet>
