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
package com.amazon.kinesis.streaming.agent.tailing;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.config.ConfigurationException;

/**
 * Factory to create specific instances of {@link FileFlow} based
 * on the configuration type.
 * Currently supported implementations:
 * <ul>
 *   <li>{@link FirehoseFileFlow}</li>
 * </ul>
 */
public class FileFlowFactory {
    /**
     *
     * @param config
     * @return
     * @throws ConfigurationException If the configuration does not correspond
     *         to a known {@link FileFlow} type.
     */
    public FileFlow<?> getFileFlow(AgentContext context, Configuration config) throws ConfigurationException {
        if(config.containsKey(FirehoseConstants.DESTINATION_KEY)) {
            return getFirehoseFileflow(context, config);
        }
        if(config.containsKey(KinesisConstants.DESTINATION_KEY)) {
        	return getKinesisFileflow(context, config);
        }
        throw new ConfigurationException("Could not create flow from the given configuration. Could not recognize flow type.");
    }

    protected FirehoseFileFlow getFirehoseFileflow(AgentContext context, Configuration config) {
        return new FirehoseFileFlow(context, config);
    }
    
    protected KinesisFileFlow getKinesisFileflow(AgentContext context, Configuration config) {
    	return new KinesisFileFlow(context, config);
    }
}
