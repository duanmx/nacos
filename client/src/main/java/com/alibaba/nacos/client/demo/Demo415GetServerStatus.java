/*
 * Copyright 1999-2026 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.client.demo;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;

import java.util.Properties;

/**
 * 4.15 获取服务端状态。
 *
 * <p>调用链路：
 * <pre>
 *   NamingService.getServerStatus()
 *     → clientProxy.serverHealthy()
 *       → grpcClientProxy.serverHealthy() || httpClientProxy.serverHealthy()
 *         → 任一通信通道健康即返回 true
 * </pre>
 *
 * <p>源码：{@link com.alibaba.nacos.client.naming.NacosNamingService#getServerStatus}
 *        {@link com.alibaba.nacos.client.naming.remote.NamingClientProxyDelegate#serverHealthy}
 */
public class Demo415GetServerStatus {

    private static final String SERVER_ADDR = "127.0.0.1:8848";

    public static void main(String[] args) throws NacosException {
        Properties properties = new Properties();
        properties.setProperty("serverAddr", SERVER_ADDR);
        properties.setProperty("namespace", "public");
        NamingService naming = NamingFactory.createNamingService(properties);

        System.out.println("--- 4.15 获取服务端状态 ---");

        String status = naming.getServerStatus();
        System.out.println("  getServerStatus() → " + status);

        if ("UP".equals(status)) {
            System.out.println("  服务端健康");
        } else {
            System.out.println("  服务端不健康或网络异常");
        }

        naming.shutDown();
    }
}
