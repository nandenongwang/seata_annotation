/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.server;

import io.seata.common.XID;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.common.util.NetUtil;
import io.seata.common.util.StringUtils;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.rpc.netty.NettyRemotingServer;
import io.seata.core.rpc.netty.NettyServerConfig;
import io.seata.server.coordinator.DefaultCoordinator;
import io.seata.server.lock.LockerManagerFactory;
import io.seata.server.metrics.MetricsManager;
import io.seata.server.session.SessionHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static io.seata.spring.boot.autoconfigure.StarterConstants.REGEX_SPLIT_CHAR;
import static io.seata.spring.boot.autoconfigure.StarterConstants.REGISTRY_PREFERED_NETWORKS;

/**
 * The type Server.
 *
 * @author slievrly
 */
public class Server {
    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void start(String[] args) {
        // create logger
        final Logger logger = LoggerFactory.getLogger(Server.class);

        //region 解析命令行参数
        //initialize the parameter parser
        //Note that the parameter parser should always be the first line to execute.
        //Because, here we need to parse the parameters needed for startup.
        ParameterParser parameterParser = new ParameterParser(args);
        //endregion

        //region 初始化度量组件
        //initialize the metrics
        MetricsManager.get().init();
        //endregion

        System.setProperty(ConfigurationKeys.STORE_MODE, parameterParser.getStoreMode());

        //region 创建netty server
        ThreadPoolExecutor workingThreads = new ThreadPoolExecutor(NettyServerConfig.getMinServerPoolSize(),
                NettyServerConfig.getMaxServerPoolSize(), NettyServerConfig.getKeepAliveTime(), TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(NettyServerConfig.getMaxTaskQueueSize()),
                new NamedThreadFactory("ServerHandlerThread", NettyServerConfig.getMaxServerPoolSize()), new ThreadPoolExecutor.CallerRunsPolicy());

        NettyRemotingServer nettyRemotingServer = new NettyRemotingServer(workingThreads);
        //endregion

        //region 根据server node ID设置ID生成器的机器ID、初始化session管理组件、初始化全局锁管理组件、初始化事务协调器并注册成server入栈请求处理器
        UUIDGenerator.init(parameterParser.getServerNode());
        //log store mode : file, db, redis
        SessionHolder.init(parameterParser.getSessionStoreMode());
        LockerManagerFactory.init(parameterParser.getLockStoreMode());
        DefaultCoordinator coordinator = DefaultCoordinator.getInstance(nettyRemotingServer);
        coordinator.init();
        nettyRemotingServer.setHandler(coordinator);
        //endregion

        //region 设置server关闭钩子【默认使用事务协调器、为通知其进行收尾工作】
        // let ServerRunner do destroy instead ShutdownHook, see https://github.com/seata/seata/issues/4028
        ServerRunner.addDisposable(coordinator);
        //endregion

        //region 设置XID生成需要的server ip地址
        //127.0.0.1 and 0.0.0.0 are not valid here.
        if (NetUtil.isValidIp(parameterParser.getHost(), false)) {
            XID.setIpAddress(parameterParser.getHost());
        } else {
            String preferredNetworks = ConfigurationFactory.getInstance().getConfig(REGISTRY_PREFERED_NETWORKS);
            if (StringUtils.isNotBlank(preferredNetworks)) {
                XID.setIpAddress(NetUtil.getLocalIp(preferredNetworks.split(REGEX_SPLIT_CHAR)));
            } else {
                XID.setIpAddress(NetUtil.getLocalIp());
            }
        }
        //endregion

        //region 初始化并启动netty server开始对外服务
        nettyRemotingServer.init();
        //endregion
    }
}
