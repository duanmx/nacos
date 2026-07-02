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
 * 4.9 批量注销服务实例。
 *
 * <p>调用链路同批量注册，走 grpcClientProxy.batchDeregisterService()。
 *
 * <p>源码：{@link com.alibaba.nacos.client.naming.NacosNamingService#batchDeregisterInstance}
 */
public class Demo49BatchDeregister {

    private static final String SERVER_ADDR = "127.0.0.1:8848";

    public static void main(String[] args) throws NacosException {
        Properties properties = new Properties();
        properties.setProperty("serverAddr", SERVER_ADDR);
        properties.setProperty("namespace", "public");
        NamingService naming = NamingFactory.createNamingService(properties);

        System.out.println("--- 4.9 批量注销服务实例 ---");

        // 先批量注册
        List<Instance> toRegister = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            Instance inst = new Instance();
            inst.setIp("192.168.1." + (100 + i));
            inst.setPort(8080);
            inst.setClusterName("BJ");
            inst.setEphemeral(true);
            toRegister.add(inst);
        }
        naming.batchRegisterInstance("batch-demo-service", "DEFAULT_GROUP", toRegister);
        sleep(500);

        // 查当前实例
        List<Instance> instances = naming.getAllInstances("batch-demo-service");
        System.out.println("  注销前：batch-demo-service 有 " + instances.size() + " 个实例");

        // 批量注销前 2 个
        List<Instance> toRemove = new ArrayList<>();
        for (int i = 0; i < Math.min(2, instances.size()); i++) {
            Instance inst = instances.get(i);
            Instance removeInst = new Instance();
            removeInst.setIp(inst.getIp());
            removeInst.setPort(inst.getPort());
            removeInst.setClusterName(inst.getClusterName());
            removeInst.setEphemeral(inst.isEphemeral());
            toRemove.add(removeInst);
        }

        if (!toRemove.isEmpty()) {
            naming.batchDeregisterInstance("batch-demo-service", "DEFAULT_GROUP", toRemove);
            System.out.println("  batchDeregisterInstance → 注销了 " + toRemove.size() + " 个实例");
        }

        // 验证
        sleep(500);
        List<Instance> after = naming.getAllInstances("batch-demo-service");
        System.out.println("  注销后：batch-demo-service 有 " + after.size() + " 个实例");

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
