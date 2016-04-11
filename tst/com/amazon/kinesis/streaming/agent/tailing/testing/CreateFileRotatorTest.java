/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing.testing;

import java.nio.file.Path;

/**
 * Unit tests for {@link CreateFileRotator}.
 */
public class CreateFileRotatorTest extends FileRotatorTestBase {

    @Override
    protected FileRotator getFileRotator(Path root, String prefix) {
        return new CreateFileRotator(root, prefix);
    }
}