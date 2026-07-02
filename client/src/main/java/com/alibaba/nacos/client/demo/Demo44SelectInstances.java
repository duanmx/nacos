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
 * 4.4 获取健康或不健康实例列表。
 *
 * <p>与 getAllInstances 的区别：
 * getAllInstances 返回所有实例（含不健康的），selectInstances 可按 healthy 过滤。
 *
 * <p>源码：{@link com.alibaba.nacos.api.naming.NamingService#selectInstances}
 */
public class Demo44SelectInstances {

    private static final String SERVER_ADDR = "127.0.0.1:8848";

    public static void main(String[] args) throws NacosException {
        Properties properties = new Properties();
        properties.setProperty("serverAddr", SERVER_ADDR);
        properties.setProperty("namespace", "public");
        NamingService naming = NamingFactory.createNamingService(properties);

        System.out.println("--- 4.4 获取健康/不健康实例 ---");

        // 先注册实例
        naming.registerInstance("demo-service", "192.168.1.1", 8080, "BJ");
        naming.registerInstance("demo-service", "192.168.1.2", 8080, "SH");
        sleep(500);

        // healthy=true → 只返回健康实例
        List<Instance> healthy = naming.selectInstances("demo-service", true);
        System.out.println("  selectInstances(serviceName, healthy=true) → "
            + healthy.size() + " 个健康实例");

        // healthy=false → 只返回不健康实例
        List<Instance> unhealthy = naming.selectInstances("demo-service", false);
        System.out.println("  selectInstances(serviceName, healthy=false) → "
            + unhealthy.size() + " 个不健康实例");

        // 指定集群 + healthy
        List<Instance> healthyInCluster = naming.selectInstances("demo-service",
            Arrays.asList("BJ"), true);
        System.out.println("  selectInstances(serviceName, clusters=[BJ], healthy=true) → "
            + healthyInCluster.size() + " 个 BJ 集群健康实例");

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
