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
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;

import java.util.EventListener;
import java.util.Properties;

/**
 * 4.1 注册实例。
 *
 * <p>调用链路：
 * <pre>
 *   NamingService.registerInstance(serviceName, groupName, instance)
 *     → clientProxy.registerService(serviceName, groupName, instance)
 *       → NamingClientProxyDelegate.getExecuteClientProxy(instance)
 *         → 临时实例 → grpcClientProxy.registerService()
 *         → 持久实例 → 视服务端能力决定 grpcClientProxy 或 httpClientProxy
 * </pre>
 *
 * <p>源码：{@link com.alibaba.nacos.client.naming.NacosNamingService#registerInstance}
 */
public class Demo41RegisterInstance {

    private static final String SERVER_ADDR = "127.0.0.1:8848";

    public static void main(String[] args) throws NacosException, InterruptedException {
        Properties properties = new Properties();
        properties.setProperty("serverAddr", SERVER_ADDR);
        properties.setProperty("namespace", "public");
        properties.setProperty("username", "nacos");
        properties.setProperty("password", "nacos");
        NamingService naming = NamingFactory.createNamingService(properties);

        System.out.println("--- 4.1 注册实例 ---");

        // 方式1：最简注册（ip + port）
        naming.registerInstance("demo-service", "192.168.1.1", 8080);
        System.out.println("  方式1：registerInstance(serviceName, ip, port) → 默认组 DEFAULT_GROUP");



        Thread.sleep(6000);

//        // 方式2：指定分组
//        naming.registerInstance("demo-service", "MY_GROUP", "192.168.1.2", 8080);
//        System.out.println("  方式2：registerInstance(serviceName, groupName, ip, port) → 指定组 MY_GROUP");
//
//        // 方式3：指定集群
//        naming.registerInstance("demo-service", "192.168.1.3", 8080, "BJ");
//        System.out.println("  方式3：registerInstance(serviceName, ip, port, clusterName) → 指定集群 BJ");
//
//        // 方式4：完整 Instance 对象（最灵活）
//        Instance instance = new Instance();
//        instance.setIp("192.168.1.4");
//        instance.setPort(8080);
//        instance.setWeight(2.0);               // 权重（负载均衡用）
//        instance.setHealthy(true);             // 健康状态
//        instance.setEphemeral(true);           // 临时实例（默认 true，走 gRPC）
//        instance.setClusterName("SH");         // 集群名
//        instance.addMetadata("version", "1.0"); // 自定义元数据
//        naming.registerInstance("demo-service", "MY_GROUP", instance);
//        System.out.println("  方式4：registerInstance(serviceName, groupName, instance) → 完整 Instance 对象");

        naming.shutDown();
    }
}
