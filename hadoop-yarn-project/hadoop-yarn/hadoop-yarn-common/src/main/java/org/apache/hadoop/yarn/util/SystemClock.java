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
package org.apache.hadoop.yarn.util;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link Clock} that gives the current time from the system
 * clock in milliseconds.
 * <p>
 * NOTE: Do not use this to calculate a duration of expire or interval to sleep,
 * because it will be broken by settimeofday. Please use {@link MonotonicClock}
 * instead.
 */
@Public
@Stable
public final class SystemClock implements Clock {
    private static final Logger LOG = LoggerFactory.getLogger(SystemClock.class);


    private static final SystemClock INSTANCE = new SystemClock();

    public static SystemClock getInstance() {
        return INSTANCE;
    }

    @Deprecated
    public SystemClock() {
        // do nothing
    }

    public long getTime() {
        LOG.info("系统时钟获取当前的时间戳");

        String className = Thread.currentThread().getStackTrace()[2].getClassName();
        String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();

        LOG.info("系统时钟获取当前的时间戳,调用类名 ====={}  方法名称 ====== {}", className, methodName);

        return System.currentTimeMillis();
    }
}