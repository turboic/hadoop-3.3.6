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

package org.apache.hadoop.yarn.server.router;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebAppUtil;
import org.apache.hadoop.yarn.server.router.clientrm.RouterClientRMService;
import org.apache.hadoop.yarn.server.router.rmadmin.RouterRMAdminService;
import org.apache.hadoop.yarn.server.router.webapp.RouterWebApp;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.WebApps.Builder;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.util.WebServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * The router is a stateless YARN component which is the entry point to the
 * cluster. It can be deployed on multiple nodes behind a Virtual IP (VIP) with
 * a LoadBalancer.
 * <p>
 * The Router exposes the ApplicationClientProtocol (RPC and REST) to the
 * outside world, transparently hiding the presence of ResourceManager(s), which
 * allows users to request and update reservations, submit and kill
 * applications, and request status on running applications.
 * <p>
 * In addition, it exposes the ResourceManager Admin API.
 * <p>
 * This provides a placeholder for throttling mis-behaving clients (YARN-1546)
 * and masks the access to multiple RMs (YARN-3659).
 */
public class Router extends CompositeService {

    private static final Logger LOG = LoggerFactory.getLogger(Router.class);
    private static CompositeServiceShutdownHook routerShutdownHook;
    private Configuration conf;
    private AtomicBoolean isStopping = new AtomicBoolean(false);
    private JvmPauseMonitor pauseMonitor;
    @VisibleForTesting
    protected RouterClientRMService clientRMProxyService;
    @VisibleForTesting
    protected RouterRMAdminService rmAdminProxyService;
    private WebApp webApp;
    @VisibleForTesting
    protected String webAppAddress;

    /**
     * Priority of the Router shutdown hook.
     */
    public static final int SHUTDOWN_HOOK_PRIORITY = 30;

    private static final String METRICS_NAME = "Router";

    public Router() {
        super(Router.class.getName());
    }

    protected void doSecureLogin() throws IOException {
        // TODO YARN-6539 Create SecureLogin inside Router
    }

    @Override
    protected void serviceInit(Configuration config) throws Exception {

        LOG.info("路由服务Route初始化,配置信息 ===== {}", config);
        this.conf = config;
        // ClientRM Proxy
        clientRMProxyService = createClientRMProxyService();
        addService(clientRMProxyService);
        // RMAdmin Proxy
        rmAdminProxyService = createRMAdminProxyService();
        addService(rmAdminProxyService);
        // WebService
        webAppAddress = WebAppUtils.getWebAppBindURL(this.conf, YarnConfiguration.ROUTER_BIND_HOST, WebAppUtils.getRouterWebAppURLWithoutScheme(this.conf));
        // Metrics
        DefaultMetricsSystem.initialize(METRICS_NAME);
        JvmMetrics jm = JvmMetrics.initSingleton("Router", null);
        pauseMonitor = new JvmPauseMonitor();
        addService(pauseMonitor);
        jm.setPauseMonitor(pauseMonitor);

        WebServiceClient.initialize(config);
        super.serviceInit(conf);
    }

    @Override
    protected void serviceStart() throws Exception {
        try {
            doSecureLogin();
        } catch (IOException e) {
            throw new YarnRuntimeException("Failed Router login", e);
        }
        startWepApp();
        super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {
        if (webApp != null) {
            webApp.stop();
        }
        if (isStopping.getAndSet(true)) {
            return;
        }
        super.serviceStop();
        DefaultMetricsSystem.shutdown();
        WebServiceClient.destroy();
    }

    protected void shutDown() {
        new Thread() {
            @Override
            public void run() {
                Router.this.stop();
            }
        }.start();
    }

    protected RouterClientRMService createClientRMProxyService() {
      LOG.info("创建客户端资源管理代理服务");
      return new RouterClientRMService();
    }

    protected RouterRMAdminService createRMAdminProxyService() {
        return new RouterRMAdminService();
    }

    @Private
    public WebApp getWebapp() {
        return this.webApp;
    }

    @VisibleForTesting
    public void startWepApp() {

        LOG.info("启动Web应用程序");

        RMWebAppUtil.setupSecurityAndFilters(conf, null);

        Builder<Object> builder = WebApps.$for("cluster", null, null, "ws").with(conf).at(webAppAddress);
        webApp = builder.start(new RouterWebApp(this));
    }

    public static void main(String[] argv) {

      LOG.info("运行了Route的main方法");


      Configuration conf = new YarnConfiguration();
      LOG.info("创建yarn配置 === {}",conf);
        Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
      LOG.info("设置线程异常捕获处理机制");
        StringUtils.startupShutdownMessage(Router.class, argv, LOG);
        Router router = new Router();

      LOG.info("创建了Route对象");

      try {

            // Remove the old hook if we are rebooting.
            if (null != routerShutdownHook) {
              LOG.info("重启的时候移除线程钩子");

              ShutdownHookManager.get().removeShutdownHook(routerShutdownHook);
            }

            routerShutdownHook = new CompositeServiceShutdownHook(router);
            ShutdownHookManager.get().addShutdownHook(routerShutdownHook, SHUTDOWN_HOOK_PRIORITY);

        LOG.info("路由初始化配置 ==== {}",conf);

        router.init(conf);
            router.start();
        LOG.info("路由服务启动了 ==== {}",conf);

      } catch (Throwable t) {
            LOG.error("Error starting Router", t);
            System.exit(-1);
        }
    }
}
