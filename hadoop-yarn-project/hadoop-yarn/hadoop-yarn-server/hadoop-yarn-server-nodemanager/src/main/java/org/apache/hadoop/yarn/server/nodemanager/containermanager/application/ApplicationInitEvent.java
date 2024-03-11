/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.application;

import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationInitEvent extends ApplicationEvent {


    private static final Logger LOG = LoggerFactory.getLogger(ApplicationInitEvent.class);


    private final Map<ApplicationAccessType, String> applicationACLs;
    private final LogAggregationContext logAggregationContext;

    public ApplicationInitEvent(ApplicationId appId, Map<ApplicationAccessType, String> acls) {
        this(appId, acls, null);
    }

    public ApplicationInitEvent(ApplicationId appId, Map<ApplicationAccessType, String> acls, LogAggregationContext logAggregationContext) {
        super(appId, ApplicationEventType.INIT_APPLICATION);
        this.applicationACLs = acls;
        this.logAggregationContext = logAggregationContext;
        LOG.info("创建应用初始化事件");

    }

    public Map<ApplicationAccessType, String> getApplicationACLs() {
        return this.applicationACLs;
    }

    public LogAggregationContext getLogAggregationContext() {
        return this.logAggregationContext;
    }
}
