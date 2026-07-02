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
import com.alibaba.nacos.api.SystemPropertyKeyConst;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.selector.SelectorFactory;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.env.SourceType;
import com.alibaba.nacos.client.utils.ContextPathUtil;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.client.utils.TemplateUtils;
import com.alibaba.nacos.client.utils.TenantUtil;
import com.alibaba.nacos.common.json.JsonAdapterLogUtils;
import com.alibaba.nacos.common.utils.StringUtils;

/**
 * Naming 客户端初始化工具类 —— 提供 namespace 解析、webContext 初始化、序列化预注册等功能。
 *
 * <h2>核心方法</h2>
 * <ul>
 *   <li><b>initNamespaceForNaming</b> —— 从多种配置源中解析 namespace（优先级：
 *       cloud namespace → JVM namespace → properties namespace → 默认 public）</li>
 *   <li><b>initWebRootContext</b> —— 初始化 HTTP API 的基础 URL 路径</li>
 *   <li><b>initSerialization</b> —— 预加载 SelectorFactory 子类型，
 *       解决 classloader 延迟加载导致的 Jackson 反序列化失败问题</li>
 * </ul>
 *
 * @author liaochuntao
 * @author deshao
 */
public class InitUtils {
    
    /**
     * Add a difference to the name naming. This method simply initializes the namespace for Naming. Config
     * initialization is not the same, so it cannot be reused directly.
     *
     * @param properties properties
     * @return namespace
     */
    public static String initNamespaceForNaming(NacosClientProperties properties) {
        String tmpNamespace = null;
        
        String isUseCloudNamespaceParsing =
            properties.getProperty(PropertyKeyConst.IS_USE_CLOUD_NAMESPACE_PARSING,
                properties.getProperty(
                    SystemPropertyKeyConst.IS_USE_CLOUD_NAMESPACE_PARSING,
                    String.valueOf(Constants.DEFAULT_USE_CLOUD_NAMESPACE_PARSING)));
        
        if (Boolean.parseBoolean(isUseCloudNamespaceParsing)) {
            
            tmpNamespace = TenantUtil.getUserTenantForAns();
            LogUtils.NAMING_LOGGER.info("initializer namespace from ans.namespace attribute : {}",
                tmpNamespace);
            
            tmpNamespace = TemplateUtils.stringEmptyAndThenExecute(tmpNamespace, () -> {
                String namespace = properties
                    .getProperty(PropertyKeyConst.SystemEnv.ALIBABA_ALIWARE_NAMESPACE);
                LogUtils.NAMING_LOGGER.info(
                    "initializer namespace from ALIBABA_ALIWARE_NAMESPACE attribute :"
                        + namespace);
                return namespace;
            });
        }
        
        tmpNamespace = TemplateUtils.stringEmptyAndThenExecute(tmpNamespace, () -> {
            String namespace =
                properties.getPropertyFrom(SourceType.JVM, PropertyKeyConst.NAMESPACE);
            LogUtils.NAMING_LOGGER
                .info("initializer namespace from namespace attribute :" + namespace);
            return namespace;
        });
        
        if (StringUtils.isEmpty(tmpNamespace)) {
            tmpNamespace = properties.getProperty(PropertyKeyConst.NAMESPACE);
        }
        
        tmpNamespace = TemplateUtils.stringEmptyAndThenExecute(tmpNamespace,
            () -> UtilAndComs.DEFAULT_NAMESPACE_ID);
        return tmpNamespace;
    }
    
    /**
     * Init web root context.
     *
     * @param properties properties
     * @since 1.4.1
     */
    public static void initWebRootContext(NacosClientProperties properties) {
        final String webContext = properties.getProperty(PropertyKeyConst.CONTEXT_PATH);
        TemplateUtils.stringNotEmptyAndThenExecute(webContext, () -> {
            UtilAndComs.webContext = ContextPathUtil.normalizeContextPath(webContext);
            UtilAndComs.nacosUrlBase = UtilAndComs.webContext + "/v1/ns";
            UtilAndComs.nacosUrlInstance = UtilAndComs.nacosUrlBase + "/instance";
        });
    }
    
    /**
     * Register subType for serialization.
     *
     * <p>
     * Now these subType implementation class has registered in static code. But there are some problem for classloader.
     * The implementation class will be loaded when they are used, which will make deserialize before register.
     * </p>
     *
     * <p>
     * 子类实现类中的静态代码串中已经向Jackson进行了注册，但是由于classloader的原因，只有当 该子类被使用的时候，才会加载该类。这可能会导致Jackson先进性反序列化，再注册子类，从而导致 反序列化失败。
     * </p>
     */
    public static void initSerialization() {
        // TODO register in implementation class or remove subType
        SelectorFactory.preload();
        JsonAdapterLogUtils.logSelectedAdapter();
    }
}
