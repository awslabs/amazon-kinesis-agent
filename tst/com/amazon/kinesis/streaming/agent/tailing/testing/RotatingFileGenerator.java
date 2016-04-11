/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing.testing;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.amazon.kinesis.streaming.agent.Logging;
import com.google.common.util.concurrent.AbstractScheduledService;

public class RotatingFileGenerator extends AbstractScheduledService {
    private static final Logger LOGGER = Logging.getLogger(RotatingFileGenerator.class);
    private final FileRotator rotator;
    private final double recordsPerSecond;
    private final long wakeUpIntervalMillis;
    private final long rotationIntervalMillis;
    private long lastIterationTimestamp = -1;

    public RotatingFileGenerator(FileRotator rotator, double recordsPerSecond) {
        this(rotator, recordsPerSecond, 200, 90_000);
    }

    public RotatingFileGenerator(FileRotator rotator, double recordsPerSecond, long wakeUpIntervalMillis,
            long rotationIntervalMillis) {
        super();
        this.rotator = rotator;
        this.recordsPerSecond = recordsPerSecond;
        this.wakeUpIntervalMillis = wakeUpIntervalMillis;
        this.rotationIntervalMillis = rotationIntervalMillis;
    }

    @Override
    protected void runOneIteration() throws Exception {
        long recordsAtBeginning = rotator.getTotalRecordsWritten();
        if (System.currentTimeMillis() - rotator.getLastRotationTime() >= rotationIntervalMillis) {
            LOGGER.debug("Rotation inteval reached ({}ms). Triggering rotation.", System.currentTimeMillis() - rotator.getLastRotationTime());
            rotator.rotate();
        }
        long millis = lastIterationTimestamp > 0 ? (System.currentTimeMillis() - lastIterationTimestamp) : wakeUpIntervalMillis;
        int newRecords = (int) (recordsPerSecond * millis / 1000);
        newRecords -= (rotator.getTotalRecordsWritten() - recordsAtBeginning);
        if (newRecords > 0) {
            rotator.appendRecordsToLatestFile(newRecords);
            LOGGER.trace("Wrote {} records to {}", newRecords, rotator.getLatestFile());
        } else {
            LOGGER.trace("No records were written to file this time.");
        }
        lastIterationTimestamp = System.currentTimeMillis();
    }

    public long getTotalRecordsWritten() {
        return rotator.getTotalRecordsWritten();
    }

    public long getTotalBytesWritten() {
        return rotator.getTotalBytesWritten();
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(0, wakeUpIntervalMillis,
                TimeUnit.MILLISECONDS);
    }
}
