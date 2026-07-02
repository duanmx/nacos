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
import com.alibaba.nacos.api.naming.pojo.ListView;

import java.util.Properties;

/**
 * 4.12 分页获取服务列表。
 *
 * <p>调用链路：
 * <pre>
 *   NamingService.getServicesOfServer(pageNo, pageSize, groupName)
 *     → clientProxy.getServiceList(pageNo, pageSize, groupName, selector)
 *       → grpcClientProxy.getServiceList()  ← 固定走 gRPC
 * </pre>
 *
 * <p>注意：这是查询服务端上所有已注册的服务名，不是客户端订阅的。
 *
 * <p>源码：{@link com.alibaba.nacos.client.naming.NacosNamingService#getServicesOfServer}
 */
public class Demo412GetServicesOfServer {

    private static final String SERVER_ADDR = "127.0.0.1:8848";

    public static void main(String[] args) throws NacosException {
        Properties properties = new Properties();
        properties.setProperty("serverAddr", SERVER_ADDR);
        properties.setProperty("namespace", "public");
        NamingService naming = NamingFactory.createNamingService(properties);

        System.out.println("--- 4.12 分页获取服务列表 ---");

        // 第1页，每页10条
        ListView<String> services = naming.getServicesOfServer(1, 10);
        System.out.println("  getServicesOfServer(1, 10) → 总数=" + services.getCount()
            + " 当前页=" + services.getData());

        // 指定分组
        ListView<String> servicesByGroup = naming.getServicesOfServer(1, 10, "DEFAULT_GROUP");
        System.out.println("  getServicesOfServer(1, 10, groupName) → 总数="
            + servicesByGroup.getCount());

        naming.shutDown();
    }
}
