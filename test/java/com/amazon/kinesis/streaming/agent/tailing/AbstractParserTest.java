package com.amazon.kinesis.streaming.agent.tailing;

import com.amazon.kinesis.streaming.agent.Agent;
import com.amazon.kinesis.streaming.agent.extension.BracketsDataConverter;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by myltik on 18/01/2016.
 */
public class AbstractParserTest {

    private final String NGINX_LOG_FILE = "resources/example_nginx_two_line.log";

    @Test
    public void testBuildRecord() throws Exception {
        // Nginx log line example
        ClassLoader classLoader = Agent.class.getClassLoader();
        Assert.assertNotNull(classLoader);

        URL resource = classLoader.getResource(NGINX_LOG_FILE);
        Assert.assertNotNull(resource);

        Path testFilePath = Paths.get(resource.toURI());
        List<String> lines = Files.readAllLines(testFilePath, StandardCharsets.UTF_8);
        Assert.assertTrue(lines != null && lines.size() > 0);

        final String dataStr = lines.get(0);
        final byte[] expectedDataBin = ("{" + dataStr + "\n}").getBytes(); // add EOL symbol as Kinesis Agent keeps it
        ByteBuffer data = ByteBuffer.wrap(dataStr.getBytes());

        // Mock file flow to return single nginx log line string
        KinesisFileFlow flow = mock(KinesisFileFlow.class);
        when(flow.hasConverter()).thenReturn(true);
        when(flow.buildConverter()).thenReturn(new BracketsDataConverter());
        when(flow.getParserBufferSize()).thenReturn(1048576+1);
        when(flow.getRecordSplitter()).thenReturn(new SingleLineSplitter());
        when(flow.getPartitionKeyOption()).thenReturn(KinesisConstants.PartitionKeyOption.RANDOM);

        // Start parsing, basically just set test one-line file path
        KinesisParser parser = new KinesisParser(flow);
        TrackedFile file = new TrackedFile(flow, testFilePath);
        file.open(0l);
        parser.startParsingFile(file);

        // Parse record
        KinesisRecord record = parser.readRecord();
        ByteBuffer res = record.data();
        byte[] resBin = new byte[res.remaining()];
        res.get(resBin); // TODO read() instead?

        Assert.assertArrayEquals(expectedDataBin, resBin);
    }
}
