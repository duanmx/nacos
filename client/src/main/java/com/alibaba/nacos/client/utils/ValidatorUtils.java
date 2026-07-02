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

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.client.env.NacosClientProperties;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 参数校验工具类 —— 提供客户端初始化参数和运行时参数的合法性校验。
 *
 * <h2>校验项</h2>
 * <ul>
 *   <li><b>contextPath</b>：不允许包含连续斜杠（如 {@code //}），
 *       防止 URL 路径注入攻击</li>
 * </ul>
 *
 * <h2>调用方</h2>
 * <p>{@link #checkInitParam(NacosClientProperties)} 在客户端服务初始化时
 * （NacosConfigService / NacosNamingService / NacosAiService）被调用，
 * 校验用户传入的 Properties 是否合法。</p>
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
public final class ValidatorUtils {
    
    private static final Pattern CONTEXT_PATH_MATCH = Pattern.compile("(\\/)\\1+");
    
    public static void checkInitParam(NacosClientProperties properties) throws NacosException {
        checkContextPath(properties.getProperty(PropertyKeyConst.CONTEXT_PATH));
    }
    
    /**
     * Check context path.
     *
     * @param contextPath context path
     */
    public static void checkContextPath(String contextPath) {
        if (contextPath == null) {
            return;
        }
        Matcher matcher = CONTEXT_PATH_MATCH.matcher(contextPath);
        if (matcher.find()) {
            throw new IllegalArgumentException("Illegal url path expression");
        }
    }
    
}
