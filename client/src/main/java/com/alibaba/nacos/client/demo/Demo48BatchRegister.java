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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 4.8 批量注册服务实例。
 *
 * <p>调用链路：
 * <pre>
 *   NamingService.batchRegisterInstance(serviceName, groupName, instances)
 *     → clientProxy.batchRegisterService(serviceName, groupName, instances)
 *       → grpcClientProxy.batchRegisterService()  ← 固定走 gRPC
 * </pre>
 *
 * <p>注意：批量注册固定走 gRPC，不经过 getExecuteClientProxy 路由。
 *
 * <p>源码：{@link com.alibaba.nacos.client.naming.NacosNamingService#batchRegisterInstance}
 */
public class Demo48BatchRegister {

    private static final String SERVER_ADDR = "127.0.0.1:8848";

    public static void main(String[] args) throws NacosException {
        Properties properties = new Properties();
        properties.setProperty("serverAddr", SERVER_ADDR);
        properties.setProperty("namespace", "public");
        NamingService naming = NamingFactory.createNamingService(properties);

        System.out.println("--- 4.8 批量注册服务实例 ---");

        List<Instance> instances = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            Instance inst = new Instance();
            inst.setIp("192.168.1." + (100 + i));
            inst.setPort(8080);
            inst.setWeight(1.0);
            inst.setClusterName("BJ");
            inst.setEphemeral(true);
            instances.add(inst);
        }

        naming.batchRegisterInstance("batch-demo-service", "DEFAULT_GROUP", instances);
        System.out.println("  batchRegisterInstance → 注册了 " + instances.size() + " 个实例");

        // 验证注册结果
        sleep(500);
        List<Instance> registered = naming.getAllInstances("batch-demo-service");
        System.out.println("  验证：getAllInstances → " + registered.size() + " 个实例");

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
