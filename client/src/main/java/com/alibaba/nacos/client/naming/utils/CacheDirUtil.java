/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.client.naming.utils;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.common.utils.StringUtils;

import java.io.File;

import com.alibaba.nacos.client.env.NacosClientProperties;

/**
 * Cache Dir Utils.
 *
 * @author zongkang.guo
 */
public class CacheDirUtil {
    
    private static String cacheDir;
    
    private static final String JM_SNAPSHOT_PATH_PROPERTY = "JM.SNAPSHOT.PATH";
    
    private static final String FILE_PATH_NACOS = "nacos";
    
    private static final String FILE_PATH_NAMING = "naming";
    
    private static final String USER_HOME_PROPERTY = "user.home";
    
    /**
     * Init cache dir.
     *
     * <p>根据配置属性拼接 Naming 客户端的本地磁盘缓存目录，最终目录结构为：
     * <pre>
     *   {baseDir}/nacos/{namingCacheRegistryDir}/naming/{namespace}
     *   ├── baseDir                → 优先取 JM.SNAPSHOT.PATH，未设置则取 user.home
     *   ├── namingCacheRegistryDir → 可选，来自 NAMING_CACHE_REGISTRY_DIR 配置，用于多注册中心隔离
     *   └── namespace               → 命名空间，不同 namespace 的缓存目录互相隔离
     * </pre>
     *
     * <p>该缓存目录在 {@link com.alibaba.nacos.client.naming.cache.ServiceInfoHolder} 构造时初始化，
     * 用于以下两个目的：
     * <ul>
     *   <li>启动时从磁盘加载缓存（当 namingLoadCacheAtStart=true 时），实现故障恢复</li>
     *   <li>运行时将最新的 ServiceInfo 持久化到磁盘，防止客户端重启后丢失服务列表</li>
     * </ul>
     *
     * @param namespace  命名空间 ID，用于隔离不同租户的缓存
     * @param properties Nacos 客户端配置属性
     * @return 拼接完成的缓存目录绝对路径
     */
    public static String initCacheDir(String namespace, NacosClientProperties properties) {
        
        // 1. 获取缓存根目录优先项：JM.SNAPSHOT.PATH（可由 -DJM.SNAPSHOT.PATH=/path 设置）
        //    如果设置了，缓存放在此目录下；否则回退到 user.home
        String jmSnapshotPath = properties.getProperty(JM_SNAPSHOT_PATH_PROPERTY);
        
        // 2. 获取可选的注册中心子目录名（namingCacheRegistryDir）
        //    用于多注册中心场景下隔离不同注册中心的 naming 缓存，未配置时为空字符串
        String namingCacheRegistryDir = "";
        if (properties.getProperty(PropertyKeyConst.NAMING_CACHE_REGISTRY_DIR) != null) {
            namingCacheRegistryDir =
                File.separator
                    + properties.getProperty(PropertyKeyConst.NAMING_CACHE_REGISTRY_DIR);
        }
        
        // 3. 拼接最终缓存目录：{base}/nacos/{registryDir?}/naming/{namespace}
        if (!StringUtils.isBlank(jmSnapshotPath)) {
            // 3a. JM.SNAPSHOT.PATH 已设置 → 用它作为根目录
            //     例: /home/snapshot/nacos/naming/public
            //     例: /home/snapshot/nacos/custom/naming/public (带 registryDir)
            cacheDir = jmSnapshotPath + File.separator + FILE_PATH_NACOS + namingCacheRegistryDir
                + File.separator
                + FILE_PATH_NAMING + File.separator + namespace;
        } else {
            // 3b. JM.SNAPSHOT.PATH 未设置 → 回退到 user.home 作为根目录
            //     例: /home/admin/nacos/naming/public
            //     例: /home/admin/nacos/custom/naming/public (带 registryDir)
            cacheDir =
                properties.getProperty(USER_HOME_PROPERTY) + File.separator + FILE_PATH_NACOS
                    + namingCacheRegistryDir
                    + File.separator + FILE_PATH_NAMING + File.separator + namespace;
        }
        
        return cacheDir;
    }
    
    public static String getCacheDir() {
        return cacheDir;
    }
}
