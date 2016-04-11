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

import java.util.concurrent.TimeUnit;

import lombok.Getter;

import com.google.common.util.concurrent.AbstractScheduledService;

public abstract class HeartbeatService extends AbstractScheduledService {
    private final long period;
    private final TimeUnit periodUnit;
    private final AgentContext agent;
    @Getter private Object lastResult;

    public HeartbeatService(AgentContext agent, long period, TimeUnit periodUnit) {
        super();
        this.period = period;
        this.periodUnit = periodUnit;
        this.agent = agent;
    }

    @Override
    protected void runOneIteration() throws Exception {
        lastResult = heartbeat(agent);
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(period, period, periodUnit);
    }

    protected abstract Object heartbeat(AgentContext agent);
}
