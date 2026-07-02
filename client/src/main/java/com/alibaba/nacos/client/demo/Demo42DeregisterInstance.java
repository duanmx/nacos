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

import java.util.Properties;

/**
 * 4.2 注销实例。
 *
 * <p>调用链路同注册，走 clientProxy.deregisterService()。
 * 注意：注销参数必须与注册时完全一致（ip + port + clusterName）。
 *
 * <p>源码：{@link com.alibaba.nacos.client.naming.NacosNamingService#deregisterInstance}
 */
public class Demo42DeregisterInstance {

    private static final String SERVER_ADDR = "127.0.0.1:8848";

    public static void main(String[] args) throws NacosException {
        Properties properties = new Properties();
        properties.setProperty("serverAddr", SERVER_ADDR);
        properties.setProperty("namespace", "public");
        NamingService naming = NamingFactory.createNamingService(properties);

        System.out.println("--- 4.2 注销实例 ---");

        // 先注册几个实例供注销
        naming.registerInstance("demo-service", "192.168.1.1", 8080);
        naming.registerInstance("demo-service", "MY_GROUP", "192.168.1.2", 8080);
        Instance preInstance = new Instance();
        preInstance.setIp("192.168.1.4");
        preInstance.setPort(8080);
        preInstance.setClusterName("SH");
        preInstance.setEphemeral(true);
        naming.registerInstance("demo-service", "MY_GROUP", preInstance);

        // 方式1：最简注销
        naming.deregisterInstance("demo-service", "192.168.1.1", 8080);
        System.out.println("  方式1：deregisterInstance(serviceName, ip, port)");

        // 方式2：指定分组 + 集群
        naming.deregisterInstance("demo-service", "MY_GROUP", "192.168.1.2", 8080);
        System.out.println("  方式2：deregisterInstance(serviceName, groupName, ip, port)");

        // 方式3：用 Instance 对象注销（推荐，参数不容易出错）
        Instance instance = new Instance();
        instance.setIp("192.168.1.4");
        instance.setPort(8080);
        instance.setClusterName("SH");
        naming.deregisterInstance("demo-service", "MY_GROUP", instance);
        System.out.println("  方式3：deregisterInstance(serviceName, groupName, instance)");

        naming.shutDown();
    }
}
