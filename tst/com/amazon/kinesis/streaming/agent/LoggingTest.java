/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent;

import static org.testng.Assert.assertEquals;

import java.io.InputStream;
import java.nio.file.Paths;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.testng.annotations.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.amazon.kinesis.streaming.agent.Logging;
import com.amazon.kinesis.streaming.agent.Logging.Log4JNamespaceContext;
import com.amazon.kinesis.streaming.agent.testing.TestUtils.TestBase;

public class LoggingTest extends TestBase {

    @Test
    public void testUnmodifiedAppenderConfig() throws Exception {
        try (InputStream stream = getClass().getResourceAsStream("LoggingTest.log4j.xml")) {
            Document dom = Logging.getLog4JConfigurationDocument(stream, null, -1, -1);
            assertEquals(getAppenderParameter(dom, "FILE", "File"), "/var/log/agent.log");
            assertEquals(getAppenderParameter(dom, "FILE", "MaxBackupIndex"), "5");
            assertEquals(getAppenderParameter(dom, "FILE", "MaxFileSize"), "10485760");
        }
    }

    @Test
    public void testModifiedAppenderConfig() throws Exception {
        try (InputStream stream = getClass().getResourceAsStream("LoggingTest.log4j.xml")) {
            Document dom = Logging.getLog4JConfigurationDocument(stream, Paths.get("/tmp/LoggingTest.log"), 543, 98765L);
            assertEquals(getAppenderParameter(dom, "FILE", "File"), "/tmp/LoggingTest.log");
            assertEquals(getAppenderParameter(dom, "FILE", "MaxBackupIndex"), "543");
            assertEquals(getAppenderParameter(dom, "FILE", "MaxFileSize"), "98765");
        }
    }

    private String getAppenderParameter(Document log4jconfig, String appender, String parameter) throws XPathExpressionException {
        XPath xPath = XPathFactory.newInstance().newXPath();
        xPath.setNamespaceContext(new Log4JNamespaceContext());
        NodeList nodes = (NodeList) xPath.evaluate(
                "/log4j:configuration/appender[@name='" + appender + "']/param[@name='" + parameter + "']",
                log4jconfig.getDocumentElement(), XPathConstants.NODESET);
        return ((Element) nodes.item(0)).getAttribute("value");
    }

}
