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

package com.alibaba.nacos.client.naming.cache;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.client.utils.ConcurrentDiskUtil;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.api.utils.json.JsonUtils;
import com.alibaba.nacos.common.utils.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * 客户端本地服务实例磁盘缓存 —— 将 ServiceInfo 持久化到磁盘文件，支持进程重启后恢复。
 *
 * <p>相当于 Nacos 命名客户端的"离线缓存层"。当进程重启时，如果服务端不可达，
 * 可以从磁盘中读取上一次缓存的实例列表，保证容灾场景下的服务可用性。</p>
 *
 * <h2>数据流转</h2>
 * <pre>{@code
 *   ServiceInfoHolder.processServiceInfo()
 *     → DiskCache.write(serviceInfo, cacheDir)      // 每次收到新数据时异步存入磁盘
 *
 *   进程重启
 *     → DiskCache.read(cacheDir)                     // 读取所有缓存文件
 *     → parseServiceInfoFromCache(file)              // 逐文件解析
 *     → 重建 ServiceInfo Map → 恢复到内存缓存
 * }</pre>
 *
 * <h2>文件格式</h2>
 * <ul>
 *   <li>文件名：URL Encoded 的 serviceKey（如 {@code default@@group@@service}）</li>
 *   <li>文件内容：每行一个 JSON 对象（可能是完整的 ServiceInfo 或单个 Instance）</li>
 *   <li>写操作使用 {@link ConcurrentDiskUtil} 保证并发安全</li>
 *   <li>忽略以 {@code meta} / {@code special-url} 后缀的特殊文件</li>
 * </ul>
 *
 * <h2>关键协作者</h2>
 * <ul>
 *   <li><b>ServiceInfoHolder</b> —— 每当 processServiceInfo() 更新本地缓存后，
 *       触发异步写入磁盘</li>
 *   <li><b>ServiceInfoDiskCacheRefresher</b> —— 定期将内存中的实例列表增量刷新到磁盘，
 *       弥补异步写入可能丢失的增量</li>
 *   <li><b>ConcurrentDiskUtil</b> —— 提供线程安全的文件操作（原子写入 + rename）</li>
 * </ul>
 *
 * <h2>生命周期</h2>
 * <p>本类为纯静态工具类，无状态，无需初始化/关闭。
 * 缓存目录由调用方（ServiceInfoHolder / FailoverReactor）通过
 * {@code CacheDirUtil.initCacheDir()} 创建。</p>
 *
 * @author xuanyin
 */
public class DiskCache {
    
    /**
     * 写入服务实例信息到磁盘。
     *
     * <p>调用方：ServiceInfoHolder.processServiceInfo() —— 每次收到服务端推送的
     * 新 ServiceInfo 后异步调用。写失败不影响主流程（只记录日志）。</p>
     *
     * <p>文件以 URL Encoded 的 serviceKey 命名，内容为 ServiceInfo 的 JSON 序列化。
     * 使用 ConcurrentDiskUtil 保证并发写入时的原子性（写入临时文件 → rename）。
     * </p>
     *
     * @param dom 服务实例信息（包含实例列表的完整 ServiceInfo）
     * @param dir 缓存目录路径
     */
    public static void write(ServiceInfo dom, String dir) {
        writeWithResult(dom, dir);
    }
    
    /**
     * 写入服务实例到磁盘并返回成功/失败。
     *
     * @param dom 服务实例信息
     * @param dir 缓存目录路径
     * @return true 写入成功，false 写入失败（异常被捕获，不影响主流程）
     */
    static boolean writeWithResult(ServiceInfo dom, String dir) {
        
        try {
            // Step 1: 确保缓存目录存在
            makeSureCacheDirExists(dir);
            
            // Step 2: 创建目标文件（以 URL Encoded 的 serviceKey 命名）
            File file = new File(dir, dom.getKeyEncoded());
            createFileIfAbsent(file, false);
            
            // Step 3: 优先使用服务端原始 JSON，回退到 JSON 序列化
            StringBuilder keyContentBuffer = new StringBuilder();
            String json = dom.getJsonFromServer();
            if (StringUtils.isEmpty(json)) {
                json = JsonUtils.toJson(dom);
            }
            keyContentBuffer.append(json);
            
            // Step 4: 并发安全的原子写入（写入临时文件 → rename）
            // Use the concurrent API to ensure the consistency.
            ConcurrentDiskUtil.writeFileContent(file, keyContentBuffer.toString(),
                StandardCharsets.UTF_8.name());
            return true;
        } catch (Throwable e) {
            NAMING_LOGGER.error("[NA] failed to write cache for dom:" + dom.getName(), e);
            return false;
        }
    }
    
    /** 获取系统行分隔符（用于跨平台兼容）。 */
    public static String getLineSeparator() {
        return System.getProperty("line.separator");
    }
    
    /**
     * 从磁盘读取所有缓存的 ServiceInfo —— 用于进程重启后恢复实例列表。
     *
     * <p>调用方：</p>
     * <ul>
     *   <li>ServiceInfoHolder 构造器 —— 初始化时恢复上次缓存的实例列表</li>
     *   <li>FailoverReactor —— 容灾场景下读取备用数据源</li>
     * </ul>
     *
     * <h3>处理流程（3 步）</h3>
     * <ol>
     *   <li>列出缓存目录下所有文件</li>
     *   <li>对每个文件调用 parseServiceInfoFromCache() 解析 ServiceInfo</li>
     *   <li>合并所有文件的结果到一个 Map（key = serviceKey）</li>
     * </ol>
     *
     * @param cacheDir 缓存文件目录路径
     * @return serviceKey → ServiceInfo 的映射（可能为空 Map，不为 null）
     */
    public static Map<String, ServiceInfo> read(String cacheDir) {
        Map<String, ServiceInfo> domMap = new HashMap<>(16);
        try {
            File[] files = makeSureCacheDirExists(cacheDir).listFiles();
            if (files == null || files.length == 0) {
                return domMap;
            }
            
            for (File file : files) {
                if (!file.isFile()) {
                    continue;
                }
                // 逐个文件解析并合并
                domMap.putAll(parseServiceInfoFromCache(file));
            }
        } catch (Throwable e) {
            NAMING_LOGGER.error("[NA] failed to read cache file", e);
        }
        
        return domMap;
    }
    
    /**
     * 从单个缓存文件解析 ServiceInfo。
     *
     * <p>支持两种格式：</p>
     * <ul>
     *   <li><b>新格式（v2+）</b>：整个文件是一行完整的 ServiceInfo JSON，包含所有实例</li>
     *   <li><b>旧格式（v1）</b>：每行一个 Instance JSON，逐行合并到 hosts 列表中</li>
     * </ul>
     * 解析时优先使用新格式（JSON 以 `{` 开头且 name 不为空），回退到旧格式。
     *
     * <p>忽略以 {@code meta} 和 {@code special-url} 结尾的元数据文件。</p>
     *
     * @param file 缓存文件
     * @return 包含一个元素的 Map（key = serviceKey）
     */
    public static Map<String, ServiceInfo> parseServiceInfoFromCache(File file)
        throws UnsupportedEncodingException {
        Map<String, ServiceInfo> result = new HashMap<>(1);
        // URL Decode 文件名恢复原始 serviceKey
        String fileName = URLDecoder.decode(file.getName(), "UTF-8");
        // 跳过元数据文件和特殊 URL 文件
        if (!(fileName.endsWith(Constants.SERVICE_INFO_SPLITER + "meta") || fileName
            .endsWith(Constants.SERVICE_INFO_SPLITER + "special-url"))) {
            ServiceInfo dom = new ServiceInfo(fileName);
            List<Instance> ips = new ArrayList<>();
            dom.setHosts(ips);
            ServiceInfo newFormat = null;
            try (BufferedReader reader = new BufferedReader(
                new StringReader(ConcurrentDiskUtil.getFileContent(file,
                    StandardCharsets.UTF_8.name())))) {
                
                String json;
                while ((json = reader.readLine()) != null) {
                    try {
                        // 判断是否为新格式：以 { 开头的完整 ServiceInfo JSON
                        if (!json.startsWith("{")) {
                            continue;
                        }
                        newFormat = JsonUtils.toObj(json, ServiceInfo.class);
                        // 如果 name 为空，说明是旧格式的 Instance JSON
                        if (StringUtils.isEmpty(newFormat.getName())) {
                            ips.add(JsonUtils.toObj(json, Instance.class));
                        }
                    } catch (Throwable e) {
                        NAMING_LOGGER.error("[NA] error while parsing cache file: " + json, e);
                    }
                }
            } catch (Exception e) {
                NAMING_LOGGER.error("[NA] failed to read cache for dom: " + file.getName(), e);
            }
            // 优先使用新格式（完整 ServiceInfo），回退到旧格式（手动构建）
            if (newFormat != null && !StringUtils.isEmpty(newFormat.getName()) && !CollectionUtils
                .isEmpty(newFormat.getHosts())) {
                result.put(dom.getKey(), newFormat);
            } else if (!CollectionUtils.isEmpty(dom.getHosts())) {
                result.put(dom.getKey(), dom);
            }
        }
        return result;
    }
    
    /**
     * 确保文件存在，不存在则创建。
     *
     * @param file  目标文件
     * @param isDir 是否为目录（true → mkdirs，false → createNewFile）
     */
    public static void createFileIfAbsent(File file, boolean isDir) throws IOException {
        if (file.exists()) {
            return;
        }
        boolean createResult = isDir ? file.mkdirs() : file.createNewFile();
        if (!createResult && !file.exists()) {
            throw new IllegalStateException(
                "failed to create cache : " + (isDir ? "dir" : file) + file.getPath());
        }
    }
    
    /**
     * 确保缓存目录存在并返回 File 对象。
     *
     * @param dir 目录路径
     * @return 缓存目录的 File 对象
     */
    private static File makeSureCacheDirExists(String dir) throws IOException {
        File cacheDir = new File(dir);
        createFileIfAbsent(cacheDir, true);
        return cacheDir;
    }
}
