/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ContainerEvent for ContainerEventType.RESUME_CONTAINER.
 */
public class ContainerResumeEvent extends ContainerEvent {

  static final Logger LOG =
          LoggerFactory.getLogger(ContainerResumeEvent.class);

  private final String diagnostic;

  public ContainerResumeEvent(ContainerId cId,
      String diagnostic) {
    super(cId, ContainerEventType.RESUME_CONTAINER);
    this.diagnostic = diagnostic;
    LOG.info("容器恢复事件  Id===={}",cId);
    LOG.info("容器恢复事件   diagnostic===={}",diagnostic);
  }

  public String getDiagnostic() {
    return this.diagnostic;
  }
}
