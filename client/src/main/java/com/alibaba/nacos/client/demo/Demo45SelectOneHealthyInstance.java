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
import java.util.Properties;

/**
 * 4.5 获取一个健康实例（负载均衡）。
 *
 * <p>不是随机取，而是按权重做负载均衡选择。
 * Instance.weight 越大，被选中的概率越高。
 *
 * <p>源码：{@link com.alibaba.nacos.client.naming.NacosNamingService#selectOneHealthyInstance}
 */
public class Demo45SelectOneHealthyInstance {

    private static final String SERVER_ADDR = "127.0.0.1:8848";

    public static void main(String[] args) throws NacosException {
        Properties properties = new Properties();
        properties.setProperty("serverAddr", SERVER_ADDR);
        properties.setProperty("namespace", "public");
        NamingService naming = NamingFactory.createNamingService(properties);

        System.out.println("--- 4.5 获取一个健康实例（负载均衡）---");

        // 先注册实例
        naming.registerInstance("demo-service", "192.168.1.1", 8080, "BJ");
        naming.registerInstance("demo-service", "192.168.1.2", 8080, "BJ");
        sleep(500);

        // 最简用法
        Instance one = naming.selectOneHealthyInstance("demo-service");
        if (one != null) {
            System.out.println("  selectOneHealthyInstance(serviceName) → "
                + one.getIp() + ":" + one.getPort() + " (weight=" + one.getWeight() + ")");
        }

        // 指定分组 + 集群
        Instance oneInCluster = naming.selectOneHealthyInstance("demo-service",
            "DEFAULT_GROUP", Arrays.asList("BJ"));
        if (oneInCluster != null) {
            System.out.println("  selectOneHealthyInstance(serviceName, groupName, clusters=[BJ]) → "
                + oneInCluster.getIp() + ":" + oneInCluster.getPort());
        }

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
