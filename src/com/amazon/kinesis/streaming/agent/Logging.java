/*
 * Copyright 2014-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License. 
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/asl/
 *  
 * or in the "license" file accompanying this file. 
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and limitations under the License.
 */
package com.amazon.kinesis.streaming.agent;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.log4j.Level;
import org.apache.log4j.MDC;
import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.google.common.annotations.VisibleForTesting;

public class Logging {

    static class Log4JNamespaceContext implements NamespaceContext {
        private final Map<String, String> uriMap;
        private final Map<String, String> prefixMap;

        public Log4JNamespaceContext() {
            this.uriMap = new HashMap<>();
            this.prefixMap = new HashMap<>();
            add("log4j", "http://jakarta.apache.org/log4j/");
        }

        public void add(String prefix, String uri) {
            this.uriMap.put(prefix, uri);
            this.prefixMap.put(uri, prefix);
        }

        @Override
        public String getNamespaceURI(String prefix) {
            return this.uriMap.get(prefix);
        }

        @Override
        public String getPrefix(String uri) {
            return this.prefixMap.get(uri);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public Iterator getPrefixes(String arg0) {
            return this.prefixMap.keySet().iterator();
        }
    }

    private static String hostname = null;
    private static boolean initialized = false;

    public synchronized static String getHostname() {
        if (hostname == null) {
            try {
                hostname = java.net.InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                hostname = System.getenv("HOSTNAME");
            }
        }
        if (hostname == null) {
            // Something is seriously wrong...
            // Falling back to "localhost", since the value here is the least of our troubles.
            hostname = "localhost";
        }
        return hostname;
    }

    public synchronized static void initialize(Path logFile, String logLevel, int maxBackupIndex, long maxFileSize)
            throws Exception {
        initialize("custom.log4j.xml", logFile, logLevel, maxBackupIndex, maxFileSize);
    }

    public synchronized static void initialize(String config, Path logFile, String logLevel, int maxBackupIndex,
            long maxFileSize) throws Exception {
        try (InputStream configStream = Logging.class.getResourceAsStream(config)) {
            initialize(configStream, logFile, logLevel, maxBackupIndex, maxFileSize);
        }
    }

    public synchronized static void initialize(InputStream configStream, Path logFile, String logLevel,
            int maxBackupIndex, long maxFileSize) throws Exception {
        if (!initialized) {
            Document log4jconfig = getLog4JConfigurationDocument(configStream, logFile, maxBackupIndex, maxFileSize);
            DOMConfigurator.configure(log4jconfig.getDocumentElement());
            org.apache.log4j.Logger logger = org.apache.log4j.Logger.getRootLogger();

            MDC.put("hostname", getHostname());
            // Set the log level, if provided
            if (logLevel != null) {
                setLogLevel(logLevel);
            }

            if (logger.isDebugEnabled()) {
                String xml = getDocumentAsString(log4jconfig);
                if (xml != null) {
                    logger.debug("Initialized logging with the following XML:\n" + xml);
                }
            }

            // Emit any initialization errors to the log
            if (CustomLog4jFallbackErrorHandler.getErrorHeader() != null) {
                logger.error("\n" + CustomLog4jFallbackErrorHandler.getErrorHeader());
            } else {
                // Log4j creates the fallback file even if no fallback happened.
                // Clean it up here.
                Path fallbackLog = Paths.get(CustomLog4jFallbackErrorHandler.getFallbackLogFile());
                if (Files.exists(fallbackLog) && Files.size(fallbackLog) == 0)
                    Files.delete(fallbackLog);
            }
            initialized = true;
        }
    }

    @VisibleForTesting
    static Document getLog4JConfigurationDocument(InputStream configStream, Path logFile, int maxBackupIndex, long maxFileSize)
            throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

        factory.setNamespaceAware(true);
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document log4jconfig = builder.parse(configStream);

        // Set the log parameters, if provided
        if (logFile != null) {
            setAppenderParameter(log4jconfig, "FILE", "File", logFile.toString());
        }
        if (maxBackupIndex > 0) {
            setAppenderParameter(log4jconfig, "FILE", "MaxBackupIndex", Integer.toString(maxBackupIndex));
        }
        if (maxFileSize > 0) {
            setAppenderParameter(log4jconfig, "FILE", "MaxFileSize", Long.toString(maxFileSize));
        }
        return log4jconfig;
    }

    private static void setAppenderParameter(Document log4jconfig, String appender, String parameter, String newValue) throws XPathExpressionException {
        XPath xPath = XPathFactory.newInstance().newXPath();
        xPath.setNamespaceContext(new Log4JNamespaceContext());
        NodeList nodes = (NodeList) xPath.evaluate(
                "/log4j:configuration/appender[@name='" + appender + "']/param[@name='" + parameter + "']",
                log4jconfig.getDocumentElement(), XPathConstants.NODESET);
        ((Element) nodes.item(0)).setAttribute("value", newValue);
    }

    @VisibleForTesting
    static String getDocumentAsString(Document doc) {
        try {
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
            transformer.setOutputProperty(OutputKeys.METHOD, "xml");
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
            StringWriter sw = new StringWriter();
            transformer.transform(new DOMSource(doc), new StreamResult(sw));
            return sw.toString();
        } catch (TransformerException e) {
            e.printStackTrace();
            return null;
        }
    }

    private static void setLogLevel(String logLevel) {
        org.apache.log4j.Logger.getRootLogger().setLevel(Level.toLevel(logLevel));
    }

    public static Logger getLogger(Class<?> clazz) {
        return LoggerFactory.getLogger(clazz);
    }
}
