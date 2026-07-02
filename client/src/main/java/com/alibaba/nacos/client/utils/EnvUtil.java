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

import com.alibaba.nacos.api.common.Constants;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 环境标识工具类 —— 管理客户端的自识别标签（self-identification tags），
 * 用于多环境路由、同机房优先调用等场景。
 *
 * <h2>核心职责</h2>
 * <p>从服务端返回的 HTTP Response Header 中解析三类环境标签：
 * <ul>
 *   <li><b>Amory Tag</b> — 单元化路由标签（如 RZone 信息）</li>
 *   <li><b>VipServer Tag</b> — VIP 服务器标签</li>
 *   <li><b>Location Tag</b> — 地理位置标签（如同机房标识）</li>
 * </ul>
 * 标签变更时通过 LOGGER.warn 输出变更日志。</p>
 *
 * <h2>数据来源</h2>
 * <p>通过 {@link #setSelfEnv(Map)} 从 HTTP Response Header 注入。
 * 典型调用链：NamingHttpClientProxy / ConfigTransportClient 在收到
 * 服务端响应后，解析 Header 中的 {@code Amory-Tag}、
 * {@code Vipserver-Tag}、{@code Location-Tag} 字段。</p>
 *
 * @author Nacos
 */
public class EnvUtil {
    
    public static final Logger LOGGER = LogUtils.logger(EnvUtil.class);
    
    private static String selfAmoryTag;
    
    private static String selfVipserverTag;
    
    private static String selfLocationTag;
    
    public static void setSelfEnv(Map<String, List<String>> headers) {
        if (headers != null) {
            List<String> amoryTagTmp = headers.get(Constants.AMORY_TAG);
            if (amoryTagTmp == null) {
                if (selfAmoryTag != null) {
                    selfAmoryTag = null;
                    LOGGER.warn("selfAmoryTag:null");
                }
            } else {
                String amoryTagTmpStr = listToString(amoryTagTmp);
                if (!Objects.equals(amoryTagTmpStr, selfAmoryTag)) {
                    selfAmoryTag = amoryTagTmpStr;
                    LOGGER.warn("selfAmoryTag:{}", selfAmoryTag);
                }
            }
            
            List<String> vipserverTagTmp = headers.get(Constants.VIPSERVER_TAG);
            if (vipserverTagTmp == null) {
                if (selfVipserverTag != null) {
                    selfVipserverTag = null;
                    LOGGER.warn("selfVipserverTag:null");
                }
            } else {
                String vipserverTagTmpStr = listToString(vipserverTagTmp);
                if (!Objects.equals(vipserverTagTmpStr, selfVipserverTag)) {
                    selfVipserverTag = vipserverTagTmpStr;
                    LOGGER.warn("selfVipserverTag:{}", selfVipserverTag);
                }
            }
            List<String> locationTagTmp = headers.get(Constants.LOCATION_TAG);
            if (locationTagTmp == null) {
                if (selfLocationTag != null) {
                    selfLocationTag = null;
                    LOGGER.warn("selfLocationTag:null");
                }
            } else {
                String locationTagTmpStr = listToString(locationTagTmp);
                if (!Objects.equals(locationTagTmpStr, selfLocationTag)) {
                    selfLocationTag = locationTagTmpStr;
                    LOGGER.warn("selfLocationTag:{}", selfLocationTag);
                }
            }
        }
    }
    
    public static String getSelfAmoryTag() {
        return selfAmoryTag;
    }
    
    public static String getSelfVipserverTag() {
        return selfVipserverTag;
    }
    
    public static String getSelfLocationTag() {
        return selfLocationTag;
    }
    
    private static String listToString(List<String> list) {
        if (list == null || list.isEmpty()) {
            return null;
        }
        StringBuilder result = new StringBuilder();
        for (String string : list) {
            result.append(string);
            result.append(',');
        }
        return result.substring(0, result.length() - 1);
    }
}
