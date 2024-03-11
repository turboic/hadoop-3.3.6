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
package org.apache.hadoop.tools.dynamometer;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Ints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import java.util.function.Supplier;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The ApplicationMaster for Dynamometer. This will launch DataNodes in YARN
 * containers. If the RPC address of a NameNode is specified, it will configure
 * the DataNodes to talk to that NameNode. Else, a NameNode will be launched as
 * part of this YARN application. This does not implement any retry/failure
 * handling.
 * TODO: Add proper retry/failure handling
 * <p>
 * The AM will persist until it has run for a period of time equal to the
 * timeout specified or until the application is killed.
 * <p>
 * If the NameNode is launched internally, it will upload some information
 * onto the remote HDFS instance (i.e., the default FileSystem) about its
 * hostname and ports. This is in the location determined by the
 * {@link DynoConstants#DYNAMOMETER_STORAGE_DIR} and
 * {@link DynoConstants#NN_INFO_FILE_NAME} constants and is in the
 * {@link Properties} file format. This is consumed by this AM as well as the
 * {@link Client} to determine how to contact the NameNode.
 * <p>
 * Information about the location of the DataNodes is logged by the AM.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ApplicationMaster {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationMaster.class);
    private static final Random RAND = new Random();

    // Configuration
    private Configuration conf;

    // Handle to communicate with the Resource Manager
    private AMRMClientAsync<ContainerRequest> amRMClient;

    // Handle to communicate with the Node Manager
    private NMClientAsync nmClientAsync;
    // Listen to process the response from the Node Manager
    private NMCallbackHandler containerListener;
    // The collection of options passed in via the Client
    private AMOptions amOptions;

    private List<LocalResource> blockListFiles;
    private int numTotalDataNodes;
    private int numTotalDataNodeContainers;

    // Counter for completed datanodes (complete denotes successful or failed )
    private AtomicInteger numCompletedDataNodeContainers = new AtomicInteger();
    // Allocated datanode count so that we know how many datanodes has the RM
    // allocated to us
    private AtomicInteger numAllocatedDataNodeContainers = new AtomicInteger();
    // Count of failed datanodes
    private AtomicInteger numFailedDataNodeContainers = new AtomicInteger();

    // True iff the application has completed and is ready for cleanup
    // Once true, will never be false. This variable should not be accessed
    // directly but rather through the isComplete, waitForCompletion, and
    // markCompleted methods.
    private boolean completed = false;
    private final Object completionLock = new Object();

    private ByteBuffer allTokens;

    // Launch threads
    private List<Thread> launchThreads = new ArrayList<>();

    // True iff this AM should launch and manage a Namanode
    private boolean launchNameNode;
    // The service RPC address of a remote NameNode to be contacted by the
    // launched DataNodes
    private String namenodeServiceRpcAddress = "";
    // Directory to use for remote storage (a location on the remote FS which
    // can be accessed by all components)
    private Path remoteStoragePath;
    // The ACLs to view the launched containers
    private Map<ApplicationAccessType, String> applicationAcls;
    // The container the NameNode is running within
    private volatile Container namenodeContainer;
    // Map of the containers that the DataNodes are running within
    private ConcurrentMap<ContainerId, Container> datanodeContainers = new ConcurrentHashMap<>();

    // Username of the user who launched this application.
    private String launchingUser;

    /**
     * @param args Command line args
     */
    public static void main(String[] args) {
        boolean result = false;
        try {
            LOG.info("ApplicationMaster类的main方法执行,创建对象new ApplicationMaster()");
            ApplicationMaster appMaster = new ApplicationMaster();

            boolean doRun = appMaster.init(args);

            LOG.info("Initializing ApplicationMaster doRun ==== {}", doRun);
            if (!doRun) {
                System.exit(0);
            }
            result = appMaster.run();

            LOG.info("ApplicationMaster 运行结果 ==== {}", result);
        } catch (Throwable t) {
            LOG.error("Error running ApplicationMaster", t);
            System.exit(1);
        }
        if (result) {
            LOG.info("Application Master completed successfully. exiting");
            System.exit(0);
        } else {
            LOG.info("Application Master failed. exiting");
            System.exit(2);
        }
    }

    public ApplicationMaster() {
        // Set up the configuration
        conf = new YarnConfiguration();
        LOG.info("ApplicationMaster构造方法初始化YarnConfiguration");
    }

    /**
     * Parse command line options.
     *
     * @param args Command line args
     * @return Whether init successful and run should be invoked
     * @throws ParseException on error while parsing options
     */
    public boolean init(String[] args) throws ParseException {

        Options opts = new Options();
        AMOptions.setOptions(opts);
        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (args.length == 0) {
            printUsage(opts);
            throw new IllegalArgumentException("No args specified for application master to initialize");
        }

        if (cliParser.hasOption("help")) {
            printUsage(opts);
            return false;
        }

        Map<String, String> envs = System.getenv();

        remoteStoragePath = new Path(envs.get(DynoConstants.REMOTE_STORAGE_PATH_ENV));
        applicationAcls = new HashMap<>();
        applicationAcls.put(ApplicationAccessType.VIEW_APP, envs.get(DynoConstants.JOB_ACL_VIEW_ENV));
        launchingUser = envs.get(Environment.USER.name());
        if (envs.containsKey(DynoConstants.REMOTE_NN_RPC_ADDR_ENV)) {
            launchNameNode = false;
            namenodeServiceRpcAddress = envs.get(DynoConstants.REMOTE_NN_RPC_ADDR_ENV);
        } else {
            launchNameNode = true;
            // namenodeServiceRpcAddress will be set in run() once properties are
            // available
        }

        ContainerId containerId = ContainerId.fromString(envs.get(Environment.CONTAINER_ID.name()));
        ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();
        LOG.info("Application master for app: appId={}, clusterTimestamp={}, " + "attemptId={}", appAttemptID.getApplicationId().getId(), appAttemptID.getApplicationId().getClusterTimestamp(), appAttemptID.getAttemptId());

        amOptions = AMOptions.initFromParser(cliParser);

        return true;
    }

    /**
     * Helper function to print usage.
     *
     * @param opts arsed command line options
     */
    private void printUsage(Options opts) {
        new HelpFormatter().printHelp("ApplicationMaster", opts);
    }

    /**
     * Main run function for the application master.
     *
     * @return True if the application completed successfully; false if if exited
     * unexpectedly, failed, was killed, etc.
     * @throws YarnException        for issues while contacting YARN daemons
     * @throws IOException          for other issues
     * @throws InterruptedException when the thread is interrupted
     */
    public boolean run() throws YarnException, IOException, InterruptedException {
        LOG.info("Starting ApplicationMaster");


        LOG.info("ApplicationMaster类的运行方法run");

        Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
        LOG.info("获取当前用户的证书资格={}", credentials);
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        // Now remove the AM->RM token so that containers cannot access it.

        credentials.getAllTokens().removeIf((token) -> token.getKind().equals(AMRMTokenIdentifier.KIND_NAME));
        allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
        LOG.info("移除访问Token信息={}", allTokens);

        LOG.info("创建内部类={}", "RMCallbackHandler");
        AMRMClientAsync.AbstractCallbackHandler allocListener = new RMCallbackHandler();
        amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
        LOG.info("创建内部类amRMClient和资源管理器进行交互 = {}", amRMClient);

        amRMClient.init(conf);
        amRMClient.start();

        containerListener = createNMCallbackHandler();
        LOG.info("创建containerListener = {}", containerListener);

        nmClientAsync = new NMClientAsyncImpl(containerListener);
        LOG.info("创建nmClientAsync,和节点管理器进行交互 = {}", nmClientAsync);
        nmClientAsync.init(conf);
        nmClientAsync.start();

        // Register self with ResourceManager
        // This will start heartbeating to the RM
        String appMasterHostname = NetUtils.getHostname();
        LOG.info("appMasterHostname = {}", appMasterHostname);

        amRMClient.registerApplicationMaster(appMasterHostname, -1, "");
        LOG.info("向资源管理器进行注册,使用名称【{}】", appMasterHostname);

        // Supplier to use to indicate to wait-loops to stop waiting

        LOG.info("检查应用的状态是完成的");
        Supplier<Boolean> exitCritera = this::isComplete;


        Optional<Properties> namenodeProperties = Optional.empty();
        if (launchNameNode) {
            LOG.info("资源管理器准备加载管理namenode组件对象 ==== {}", true);
            ContainerRequest nnContainerRequest = setupContainerAskForRM(amOptions.getNameNodeMemoryMB(), amOptions.getNameNodeVirtualCores(), 0, amOptions.getNameNodeNodeLabelExpression());
            LOG.info("请求 Requested NameNode ask: " + nnContainerRequest.toString());
            amRMClient.addContainerRequest(nnContainerRequest);
            LOG.info("资源管理客户端添加ContainerRequest");

            // Wait for the NN container to make its information available on the
            // shared
            // remote file storage
            Path namenodeInfoPath = new Path(remoteStoragePath, DynoConstants.NN_INFO_FILE_NAME);
            LOG.info("Waiting on availability of NameNode information at " + namenodeInfoPath);

            namenodeProperties = DynoInfraUtils.waitForAndGetNameNodeProperties(exitCritera, conf, namenodeInfoPath, LOG);
            if (!namenodeProperties.isPresent()) {
                LOG.info("namenodeProperties.isPresent() " + "cleanup");
                cleanup();
                return false;
            }
            namenodeServiceRpcAddress = DynoInfraUtils.getNameNodeServiceRpcAddr(namenodeProperties.get()).toString();
            LOG.info("NameNode information: " + namenodeProperties.get() + "地址是: " + namenodeServiceRpcAddress);
            LOG.info("NameNode can be reached at: " + DynoInfraUtils.getNameNodeHdfsUri(namenodeProperties.get()).toString());
            DynoInfraUtils.waitForNameNodeStartup(namenodeProperties.get(), exitCritera, LOG);
        } else {
            LOG.info("Using remote NameNode with RPC address: " + namenodeServiceRpcAddress);
        }

        blockListFiles = Collections.synchronizedList(getDataNodeBlockListingFiles());
        numTotalDataNodes = blockListFiles.size();
        if (numTotalDataNodes == 0) {
            LOG.error("No block listing files were found! Cannot run with 0 DataNodes.");
            markCompleted();
            return false;
        }
        numTotalDataNodeContainers = (int) Math.ceil(((double) numTotalDataNodes) / Math.max(1, amOptions.getDataNodesPerCluster()));

        LOG.info("Requesting {} DataNode containers with {} MB memory, {} vcores", numTotalDataNodeContainers, amOptions.getDataNodeMemoryMB(), amOptions.getDataNodeVirtualCores());
        for (int i = 0; i < numTotalDataNodeContainers; ++i) {
            ContainerRequest datanodeAsk = setupContainerAskForRM(amOptions.getDataNodeMemoryMB(), amOptions.getDataNodeVirtualCores(), 1, amOptions.getDataNodeNodeLabelExpression());
            amRMClient.addContainerRequest(datanodeAsk);
            LOG.debug("Requested datanode ask: " + datanodeAsk.toString());
        }
        LOG.info("Finished requesting datanode containers");

        if (launchNameNode) {
            LOG.info("launchNameNode waitForNameNodeReadiness");
            DynoInfraUtils.waitForNameNodeReadiness(namenodeProperties.get(), numTotalDataNodes, true, exitCritera, conf, LOG);
        }
        LOG.info("设置完成锁,直到应用初始化完成");
        waitForCompletion();
        return cleanup();
    }

    private NMCallbackHandler createNMCallbackHandler() {
        return new NMCallbackHandler();
    }

    /**
     * Wait until the application has finished and is ready for cleanup.
     */
    private void waitForCompletion() throws InterruptedException {
        synchronized (completionLock) {
            while (!completed) {
                completionLock.wait();
            }
        }
    }

    /**
     * Check completion status of the application.
     *
     * @return True iff it has completed.
     */
    private boolean isComplete() {
        LOG.info("完成");
        synchronized (completionLock) {
            return completed;
        }
    }

    /**
     * Mark that this application should begin cleaning up and exit.
     */
    private void markCompleted() {
        LOG.info("标记完成");
        synchronized (completionLock) {
            completed = true;
            completionLock.notify();
        }
    }

    /**
     * @return True iff the application successfully completed
     */
    private boolean cleanup() {
        LOG.info("cleanup方法被调用,说明the application successfully completed");
        // Join all launched threads
        // needed for when we time out
        // and we need to release containers
        for (Thread launchThread : launchThreads) {
            try {
                LOG.info("等待线程" + launchThread.getName() + "10000秒后向下执行");
                launchThread.join(10000);
            } catch (InterruptedException e) {
                LOG.info("Exception thrown in thread join: " + e.getMessage());
                e.printStackTrace();
            }
        }

        // When the application completes, it should stop all running containers
        LOG.info("Application completed. Stopping running containers");
        nmClientAsync.stop();
        LOG.info("Application 完成. 正在停止运行的节点容器");

        // When the application completes, it should send a finish application
        // signal to the RM
        LOG.info("Application completed. Signalling finish to RM");

        LOG.info("Application 完成. 发送信号量告知资源管理器RM");

        FinalApplicationStatus appStatus;
        String appMessage = null;
        boolean success;

        LOG.info("numFailedDataNodeContainers ={}, numCompletedDataNodeContainers ={},numTotalDataNodes={}," + numFailedDataNodeContainers.get(), numCompletedDataNodeContainers.get(), numTotalDataNodes);

        if (numFailedDataNodeContainers.get() == 0 && numCompletedDataNodeContainers.get() == numTotalDataNodes) {
            appStatus = FinalApplicationStatus.SUCCEEDED;
            success = true;
            LOG.info("Application的FinalApplicationStatus 完成状态是成功的");
        } else {
            appStatus = FinalApplicationStatus.FAILED;
            appMessage = "Diagnostics: total=" + numTotalDataNodeContainers + ", completed=" + numCompletedDataNodeContainers.get() + ", allocated=" + numAllocatedDataNodeContainers.get() + ", failed=" + numFailedDataNodeContainers.get();
            success = false;
            LOG.info("Application的FinalApplicationStatus 完成状态是失败的，失败原因" + appMessage);
        }
        try {
            LOG.info("资源客户端取消应用【ApplicationMaster】的注册,传入的参数appStatus：{},appMessage:{}", appStatus, appMessage);
            amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
        } catch (YarnException | IOException ex) {
            LOG.error("Failed to unregister application", ex);
            LOG.error("失败取消注册应用 to unregister application", ex);
        }

        amRMClient.stop();
        LOG.info("amRMClient停止stop，结果success = {}", success);
        return success;
    }

    private class RMCallbackHandler extends AMRMClientAsync.AbstractCallbackHandler {

        @Override
        public void onContainersCompleted(List<ContainerStatus> completedContainers) {
            LOG.info("Got response from RM for container ask, completedCnt=" + completedContainers.size());
            for (ContainerStatus containerStatus : completedContainers) {
                String containerInfo = "containerID=" + containerStatus.getContainerId() + ", state=" + containerStatus.getState() + ", exitStatus=" + containerStatus.getExitStatus() + ", diagnostics=" + StringUtils.abbreviate(containerStatus.getDiagnostics(), 1000);
                String component;
                if (isNameNode(containerStatus.getContainerId())) {
                    component = "NAMENODE";
                } else if (isDataNode(containerStatus.getContainerId())) {
                    component = "DATANODE";
                } else {
                    LOG.error("Received container status for unknown container: " + containerInfo);
                    continue;
                }
                LOG.info("Got container status for " + component + ": " + containerInfo);

                // non complete containers should not be here
                assert (containerStatus.getState() == ContainerState.COMPLETE);

                if (component.equals("NAMENODE")) {
                    LOG.info("NameNode container completed; marking application as done");
                    markCompleted();
                }

                // increment counters for completed/failed containers
                int exitStatus = containerStatus.getExitStatus();
                int completedIdx = numCompletedDataNodeContainers.incrementAndGet();
                if (0 != exitStatus) {
                    numFailedDataNodeContainers.incrementAndGet();
                } else {
                    LOG.info("DataNode {} completed successfully, containerId={}", completedIdx, containerStatus.getContainerId());
                }
            }

            if (numCompletedDataNodeContainers.get() == numTotalDataNodeContainers) {
                LOG.info("All datanode containers completed; marking application as done");
                markCompleted();
            }
        }

        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {
            LOG.info("Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size());
            for (Container container : allocatedContainers) {
                LaunchContainerRunnable containerLauncher;
                String componentType;
                Resource resource = container.getResource();
                if (launchNameNode && resource.getMemorySize() >= amOptions.getNameNodeMemoryMB() && resource.getVirtualCores() >= amOptions.getNameNodeVirtualCores() && namenodeContainer == null) {
                    namenodeContainer = container;
                    componentType = "NAMENODE";
                    containerLauncher = new LaunchContainerRunnable(container, true);
                } else if (resource.getMemorySize() >= amOptions.getDataNodeMemoryMB() && resource.getVirtualCores() >= amOptions.getDataNodeVirtualCores() && numAllocatedDataNodeContainers.get() < numTotalDataNodes) {
                    if (launchNameNode && namenodeContainer == null) {
                        LOG.error("Received a container with following resources suited " + "for a DataNode but no NameNode container exists: " + "containerMem=" + resource.getMemorySize() + ", containerVcores=" + resource.getVirtualCores());
                        continue;
                    }
                    numAllocatedDataNodeContainers.getAndIncrement();
                    datanodeContainers.put(container.getId(), container);
                    componentType = "DATANODE";
                    containerLauncher = new LaunchContainerRunnable(container, false);
                } else {
                    LOG.warn("Received unwanted container allocation: " + container);
                    nmClientAsync.stopContainerAsync(container.getId(), container.getNodeId());
                    continue;
                }
                LOG.info("正在加载 " + componentType + " on a new container." + ", containerId=" + container.getId() + ", containerNode=" + container.getNodeId().getHost() + ":" + container.getNodeId().getPort() + ", containerNodeURI=" + container.getNodeHttpAddress() + ", containerResourceMemory=" + resource.getMemorySize() + ", containerResourceVirtualCores=" + resource.getVirtualCores());
                Thread launchThread = new Thread(containerLauncher);

                // launch and start the container on a separate thread to keep
                // the main thread unblocked
                // as all containers may not be allocated at one go.
                launchThreads.add(launchThread);
                launchThread.start();
            }
        }

        @Override
        public void onShutdownRequest() {
            markCompleted();
        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {
            LOG.info("onNodesUpdated: " + Joiner.on(",").join(updatedNodes));
        }

        @Override
        public float getProgress() {
            return 0.0f;
        }

        @Override
        public void onError(Throwable e) {
            markCompleted();
            amRMClient.stop();
        }

        @Override
        public void onContainersUpdated(List<UpdatedContainer> containers) {
            LOG.info("onContainersUpdated: " + Joiner.on(",").join(containers));
        }
    }

    private class NMCallbackHandler extends NMClientAsync.AbstractCallbackHandler {

        @Override
        public void onContainerStopped(ContainerId containerId) {
            LOG.info("节点管理回调操作,容器停止时触发,容器ContainerId"+containerId.toString());
            if (isNameNode(containerId)) {
                LOG.info("是名称节点 NameNode container stopped: " + containerId);
                namenodeContainer = null;
                markCompleted();
            } else if (isDataNode(containerId)) {
                LOG.info("是数据节点  DataNode container stopped: " + containerId);
                datanodeContainers.remove(containerId);
            } else {
                LOG.error("未知节点类型  onContainerStopped received unknown container ID: " + containerId);
            }
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
            LOG.info("NMClientAsyncImpl类进行调用：");
            LOG.info("Container Status: id=" + containerId + ", status=" + containerStatus);
        }

        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
            if (isNameNode(containerId)) {
                LOG.info("NameNode container started at ID " + containerId);
            } else if (isDataNode(containerId)) {
                LOG.info("Succeeded to start DataNode Container " + containerId);
                nmClientAsync.getContainerStatusAsync(containerId, datanodeContainers.get(containerId).getNodeId());
            } else {
                LOG.error("onContainerStarted received unknown container ID: " + containerId);
            }
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t) {
            if (isNameNode(containerId)) {
                LOG.error("Failed to start namenode container ID " + containerId, t);
                namenodeContainer = null;
                markCompleted();
            } else if (isDataNode(containerId)) {
                LOG.error("Failed to start DataNode Container " + containerId);
                datanodeContainers.remove(containerId);
                numCompletedDataNodeContainers.incrementAndGet();
                numFailedDataNodeContainers.incrementAndGet();
            } else {
                LOG.error("onStartContainerError received unknown container ID: " + containerId);
            }
        }

        @Override
        public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to query the status of Container " + containerId);
        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t) {
            if (isNameNode(containerId)) {
                LOG.error("Failed to stop NameNode container ID " + containerId);
                namenodeContainer = null;
            } else if (isDataNode(containerId)) {
                LOG.error("Failed to stop DataNode Container " + containerId);
                datanodeContainers.remove(containerId);
            } else {
                LOG.error("onStopContainerError received unknown containerID: " + containerId);
            }
        }

        @Override
        @Deprecated
        public void onContainerResourceIncreased(ContainerId containerId, Resource resource) {
            LOG.info("onContainerResourceIncreased: {}, {}", containerId, resource);
        }

        @Override
        public void onContainerResourceUpdated(ContainerId containerId, Resource resource) {
            LOG.info("onContainerResourceUpdated: {}, {}", containerId, resource);
        }

        @Override
        @Deprecated
        public void onIncreaseContainerResourceError(ContainerId containerId, Throwable t) {
            LOG.info("onIncreaseContainerResourceError: {}", containerId, t);
        }

        @Override
        public void onUpdateContainerResourceError(ContainerId containerId, Throwable t) {
            LOG.info("onUpdateContainerResourceError: {}", containerId, t);
        }
    }

    /**
     * Thread to connect to the {@link ContainerManagementProtocol} and launch the
     * container that will execute the shell command.
     */
    private class LaunchContainerRunnable implements Runnable {

        // Allocated container
        private Container container;
        private boolean isNameNodeLauncher;

        /**
         * @param lcontainer Allocated container
         * @param isNameNode True iff this should launch a NameNode
         */
        LaunchContainerRunnable(Container lcontainer, boolean isNameNode) {
            this.container = lcontainer;
            this.isNameNodeLauncher = isNameNode;
            LOG.info("加载容器线程构造方法 Container = {},是NAME_NODE节点={}",lcontainer,isNameNode);
        }

        /**
         * Get the map of local resources to be used for launching this container.
         */
        private Map<String, LocalResource> getLocalResources() {
            Map<String, LocalResource> localResources = new HashMap<>();

            Map<String, String> envs = System.getenv();
            addAsLocalResourceFromEnv(DynoConstants.CONF_ZIP, localResources, envs);
            addAsLocalResourceFromEnv(DynoConstants.START_SCRIPT, localResources, envs);
            addAsLocalResourceFromEnv(DynoConstants.HADOOP_BINARY, localResources, envs);
            addAsLocalResourceFromEnv(DynoConstants.VERSION, localResources, envs);
            addAsLocalResourceFromEnv(DynoConstants.DYNO_DEPENDENCIES, localResources, envs);
            if (isNameNodeLauncher) {
                addAsLocalResourceFromEnv(DynoConstants.FS_IMAGE, localResources, envs);
                addAsLocalResourceFromEnv(DynoConstants.FS_IMAGE_MD5, localResources, envs);
            } else {
                int blockFilesToLocalize = Math.max(1, amOptions.getDataNodesPerCluster());
                for (int i = 0; i < blockFilesToLocalize; i++) {
                    try {
                        localResources.put(DynoConstants.BLOCK_LIST_RESOURCE_PATH_PREFIX + i, blockListFiles.remove(0));
                    } catch (IndexOutOfBoundsException e) {
                        break;
                    }
                }
            }
            return localResources;
        }

        /**
         * Connects to CM, sets up container launch context for shell command and
         * eventually dispatches the container start request to the CM.
         */
        @Override
        public void run() {
            LOG.info("Setting up container launch context for containerid=" + container.getId() + ", isNameNode=" + isNameNodeLauncher);
            ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

            // Set the environment
            ctx.setEnvironment(amOptions.getShellEnv());
            ctx.setApplicationACLs(applicationAcls);

            try {
                ctx.setLocalResources(getLocalResources());

                ctx.setCommands(getContainerStartCommand());
            } catch (IOException e) {
                LOG.error("Error while configuring container!", e);
                return;
            }

            // Set up tokens for the container
            ctx.setTokens(allTokens.duplicate());

            nmClientAsync.startContainerAsync(container, ctx);
            LOG.info("Starting {}; track at: http://{}/node/containerlogs/{}/{}/", isNameNodeLauncher ? "NAMENODE" : "DATANODE", container.getNodeHttpAddress(), container.getId(), launchingUser);
        }

        /**
         * Return the command used to start this container.
         */
        private List<String> getContainerStartCommand() throws IOException {
            // Set the necessary command to execute on the allocated container
            List<String> vargs = new ArrayList<>();

            // Set executable command
            vargs.add("./" + DynoConstants.START_SCRIPT.getResourcePath());
            String component = isNameNodeLauncher ? "namenode" : "datanode";
            vargs.add(component);
            if (isNameNodeLauncher) {
                vargs.add(remoteStoragePath.getFileSystem(conf).makeQualified(remoteStoragePath).toString());
            } else {
                vargs.add(namenodeServiceRpcAddress);
                vargs.add(String.valueOf(amOptions.getDataNodeLaunchDelaySec() < 1 ? 0 : RAND.nextInt(Ints.checkedCast(amOptions.getDataNodeLaunchDelaySec()))));
            }

            // Add log redirect params
            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

            LOG.info("Completed setting up command for " + component + ": " + vargs);
            return Lists.newArrayList(Joiner.on(" ").join(vargs));
        }

        /**
         * Add the given resource into the map of resources, using information from
         * the supplied environment variables.
         *
         * @param resource       The resource to add.
         * @param localResources Map of local resources to insert into.
         * @param env            Map of environment variables.
         */
        public void addAsLocalResourceFromEnv(DynoResource resource, Map<String, LocalResource> localResources, Map<String, String> env) {
            LOG.debug("Adding resource to localResources: " + resource);
            String resourcePath = resource.getResourcePath();
            if (resourcePath == null) {
                // Default to using the file name in the path
                resourcePath = resource.getPath(env).getName();
            }
            localResources.put(resourcePath, LocalResource.newInstance(URL.fromPath(resource.getPath(env)), resource.getType(), LocalResourceVisibility.APPLICATION, resource.getLength(env), resource.getTimestamp(env)));
        }
    }

    private List<LocalResource> getDataNodeBlockListingFiles() throws IOException {
        Path blockListDirPath = new Path(System.getenv().get(DynoConstants.BLOCK_LIST_PATH_ENV));
        LOG.info("Looking for block listing files in " + blockListDirPath);
        FileSystem blockZipFS = blockListDirPath.getFileSystem(conf);
        List<LocalResource> files = new LinkedList<>();
        for (FileStatus stat : blockZipFS.listStatus(blockListDirPath, DynoConstants.BLOCK_LIST_FILE_FILTER)) {
            LocalResource blockListResource = LocalResource.newInstance(URL.fromPath(stat.getPath()), LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, stat.getLen(), stat.getModificationTime());
            files.add(blockListResource);
        }
        return files;
    }

    /**
     * Return true iff {@code containerId} represents the NameNode container.
     */
    private boolean isNameNode(ContainerId containerId) {
        return namenodeContainer != null && namenodeContainer.getId().equals(containerId);
    }

    /**
     * Return true iff {@code containerId} represents a DataNode container.
     */
    private boolean isDataNode(ContainerId containerId) {
        return datanodeContainers.containsKey(containerId);
    }

    /**
     * Setup the request that will be sent to the RM for the container ask.
     *
     * @return the setup ResourceRequest to be sent to RM
     */
    private ContainerRequest setupContainerAskForRM(int memory, int vcores, int priority, String nodeLabel) {
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(priority);

        // Set up resource type requirements
        // For now, memory and CPU are supported so we set memory and cpu
        // requirements
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemorySize(memory);
        capability.setVirtualCores(vcores);

        return new ContainerRequest(capability, null, null, pri, true, nodeLabel);
    }

}
