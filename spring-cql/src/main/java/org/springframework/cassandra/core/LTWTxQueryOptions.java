/*
 * Copyright 2013-2014 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.core;

public class LTWTxQueryOptions extends WriteOptions {

    private ConsistencyLevel serialConsistencyLevel;

    public LTWTxQueryOptions() {
    }

    public LTWTxQueryOptions(ConsistencyLevel consistencyLevel, RetryPolicy retryPolicy, ConsistencyLevel serialConsistencyLevel) {
        super(consistencyLevel, retryPolicy);
        this.serialConsistencyLevel = serialConsistencyLevel;
    }

    public LTWTxQueryOptions(ConsistencyLevel consistencyLevel, RetryPolicy retryPolicy, Integer ttl, ConsistencyLevel serialConsistencyLevel) {
        super(consistencyLevel, retryPolicy, ttl);
        this.serialConsistencyLevel = serialConsistencyLevel;
    }

    public ConsistencyLevel getSerialConsistencyLevel() {
        return serialConsistencyLevel;
    }

    public void setSerialConsistencyLevel(ConsistencyLevel serialConsistencyLevel) {
        this.serialConsistencyLevel = serialConsistencyLevel;
    }
}
