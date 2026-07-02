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

package com.alibaba.nacos.client.utils;

import com.alibaba.nacos.client.logging.NacosLogging;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * 日志工具类 —— 负责在类加载时初始化 Nacos 客户端的日志配置，
 * 并提供便捷的 {@link Logger} 获取方法。
 *
 * <h2>初始化时机</h2>
 * <p>静态块中调用 {@link NacosLogging#loadConfiguration()} 加载日志配置，
 * 确保在任何客户端组件使用 Logger 之前日志系统已就绪。</p>
 *
 * <p>同时预创建 {@code NAMING_LOGGER} —— 一个专门用于 Naming 模块的 Logger 实例。</p>
 *
 * @author <a href="mailto:huangxiaoyu1018@gmail.com">hxy1991</a>
 * @since 0.9.0
 */
public class LogUtils {
    
    public static final Logger NAMING_LOGGER;
    
    static {
        NacosLogging.getInstance().loadConfiguration();
        NAMING_LOGGER = getLogger("com.alibaba.nacos.client.naming");
    }
    
    public static Logger logger(Class<?> clazz) {
        return getLogger(clazz);
    }
    
}
