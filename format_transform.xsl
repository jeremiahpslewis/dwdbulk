<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0"
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
	xmlns:dwd="https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd"
	xmlns:kml="http://www.opengis.net/kml/2.2">

  <xsl:output method="xml" indent="yes"/>
  <xsl:strip-space elements="*"/>

  <xsl:template match="kml:Document">
    <forecast>
      <xsl:apply-templates />
    </forecast>
  </xsl:template>

  <xsl:template match="kml:ExtendedData">
    <xsl:apply-templates select="dwd:ProductDefinition/dwd:ForecastTimeSteps"/>
  </xsl:template>

	<xsl:template match="dwd:ForecastTimeSteps">
    <timeSteps>
      <xsl:for-each select="dwd:TimeStep">
	    <xsl:text>"</xsl:text><xsl:value-of select="." /><xsl:text>"</xsl:text>
	    <xsl:if test="position() != last()">
	        <xsl:text>,</xsl:text>
	    </xsl:if>
      </xsl:for-each>
    </timeSteps>
	</xsl:template>

  <xsl:template match="kml:Placemark">
  <xsl:if test="kml:name=01028">
    <station>
    <xsl:apply-templates select="dwd:ProductDefinition/dwd:ForecastTimeSteps"/>
    <xsl:attribute name="name"><xsl:value-of select="kml:name"/></xsl:attribute>
    <xsl:attribute name="description"><xsl:value-of select="kml:description"/></xsl:attribute>
    <xsl:for-each select="kml:ExtendedData/dwd:Forecast">
      <forecast>
        <xsl:attribute name="elementName"><xsl:value-of select="@dwd:elementName" /></xsl:attribute>
        <xsl:call-template name="format">
           <xsl:with-param name="text" select="normalize-space()"/>
         </xsl:call-template>
      </forecast>
    </xsl:for-each>
    </station>
  </xsl:if>
  </xsl:template>

   <xsl:template name="format">
    <xsl:param name="text"/>
	<xsl:variable name="replace" select="' '" /> 
	<xsl:variable name="with" select="','" /> 

    <xsl:choose>
      <xsl:when test="contains($text,$replace)">
        <xsl:text>"</xsl:text><xsl:value-of select="substring-before($text,$replace)"/><xsl:text>"</xsl:text>
        <xsl:value-of select="$with"/>
        <xsl:call-template name="format">
          <xsl:with-param name="text" select="substring-after($text,$replace)"/>
        </xsl:call-template>
      </xsl:when>
      <xsl:otherwise>
        <xsl:text>"</xsl:text><xsl:value-of select="$text"/><xsl:text>"</xsl:text>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

</xsl:stylesheet>