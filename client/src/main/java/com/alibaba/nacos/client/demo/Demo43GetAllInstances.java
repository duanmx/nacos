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
import com.alibaba.nacos.api.naming.pojo.Instance;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 4.3 获取全部实例。
 *
 * <p>调用链路：
 * <pre>
 *   NamingService.getAllInstances(serviceName, groupName, clusters, subscribe)
 *     → clientProxy.subscribe() 或 直接读本地缓存
 *       → subscribe=true：先订阅（gRPC 发请求），再返回本地缓存
 *       → subscribe=false：直接查服务端，不订阅
 * </pre>
 *
 * <p>源码：{@link com.alibaba.nacos.client.naming.NacosNamingService#getAllInstances}
 */
public class Demo43GetAllInstances {

    private static final String SERVER_ADDR = "127.0.0.1:8848";

    public static void main(String[] args) throws NacosException {
        Properties properties = new Properties();
        properties.setProperty("serverAddr", SERVER_ADDR);
        properties.setProperty("namespace", "public");
        NamingService naming = NamingFactory.createNamingService(properties);

        System.out.println("--- 4.3 获取全部实例 ---");

        // 先注册一些实例
        naming.registerInstance("demo-service", "192.168.1.1", 8080, "BJ");
        naming.registerInstance("demo-service", "192.168.1.2", 8080, "SH");
        sleep(500); // 等待注册完成

        // 方式1：最简查询
        List<Instance> all = naming.getAllInstances("demo-service");
        System.out.println("  方式1：getAllInstances(serviceName) → " + all.size() + " 个实例");

        // 方式2：指定分组
        List<Instance> byGroup = naming.getAllInstances("demo-service", "DEFAULT_GROUP");
        System.out.println("  方式2：getAllInstances(serviceName, groupName) → " + byGroup.size() + " 个实例");

        // 方式3：指定集群
        List<Instance> byCluster = naming.getAllInstances("demo-service", Arrays.asList("BJ"));
        System.out.println("  方式3：getAllInstances(serviceName, clusters=[BJ]) → "
            + byCluster.size() + " 个实例（只返回 BJ 集群的）");

        // 方式4：subscribe=false → 不订阅，直接查服务端
        List<Instance> noSubscribe = naming.getAllInstances("demo-service",
            "DEFAULT_GROUP", Arrays.asList("BJ"), false);
        System.out.println("  方式4：getAllInstances(..., subscribe=false) → 不订阅，直接查服务端");

        naming.shutDown();
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
