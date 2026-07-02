/*
 * Copyright 1999-2023 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.client.naming.event;

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.common.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * 实例差异结果 —— 承载新增/移除/修改三类实例变更。
 *
 * <p>由 {@link com.alibaba.nacos.client.naming.cache.InstancesDiffer#doDiff
 * InstancesDiffer.doDiff()} 计算生成，作为 {@link InstancesChangeEvent} 的
 * 差异详情字段传递给用户侧 {@link com.alibaba.nacos.api.naming.listener.EventListener}。</p>
 *
 * <p>三个列表互斥（同一个 ip:port 不会同时出现在两个列表中），
 * 通过 {@link #hasDifferent()} 快速判断是否有任何变化。</p>
 *
 * @author lideyou
 */
public class InstancesDiff {
    
    /** 新增实例列表（旧缓存无、新缓存有）。 */
    private final List<Instance> addedInstances = new ArrayList<>();
    
    /** 移除实例列表（旧缓存有、新缓存无）。 */
    private final List<Instance> removedInstances = new ArrayList<>();
    
    /** 修改实例列表（ip:port 相同但其他属性变化，如 weight/healthy）。 */
    private final List<Instance> modifiedInstances = new ArrayList<>();
    
    public InstancesDiff() {
    }
    
    public InstancesDiff(List<Instance> addedInstances, List<Instance> removedInstances,
        List<Instance> modifiedInstances) {
        setAddedInstances(addedInstances);
        setRemovedInstances(removedInstances);
        setModifiedInstances(modifiedInstances);
    }
    
    public List<Instance> getAddedInstances() {
        return addedInstances;
    }
    
    public void setAddedInstances(Collection<Instance> addedInstances) {
        this.addedInstances.clear();
        if (CollectionUtils.isNotEmpty(addedInstances)) {
            this.addedInstances.addAll(addedInstances);
        }
    }
    
    public List<Instance> getRemovedInstances() {
        return removedInstances;
    }
    
    public void setRemovedInstances(Collection<Instance> removedInstances) {
        this.removedInstances.clear();
        if (CollectionUtils.isNotEmpty(removedInstances)) {
            this.removedInstances.addAll(removedInstances);
        }
    }
    
    public List<Instance> getModifiedInstances() {
        return modifiedInstances;
    }
    
    public void setModifiedInstances(Collection<Instance> modifiedInstances) {
        this.modifiedInstances.clear();
        if (CollectionUtils.isNotEmpty(modifiedInstances)) {
            this.modifiedInstances.addAll(modifiedInstances);
        }
    }
    
    /**
     * 判断是否有任何实例变化（新增 or 移除 or 修改）。
     *
     * <p>调用方：ServiceInfoHolder.processServiceInfo() —— 有变化时才发布事件，
     * 无变化则跳过，避免无效通知。</p>
     *
     * @return true 有变化，false 无变化
     */
    public boolean hasDifferent() {
        return isAdded() || isRemoved() || isModified();
    }
    
    /**
     * 判断是否有新增实例。
     */
    public boolean isAdded() {
        return CollectionUtils.isNotEmpty(this.addedInstances);
    }
    
    /**
     * 判断是否有移除实例。
     */
    public boolean isRemoved() {
        return CollectionUtils.isNotEmpty(this.removedInstances);
    }
    
    /**
     * 判断是否有修改实例。
     */
    public boolean isModified() {
        return CollectionUtils.isNotEmpty(this.modifiedInstances);
    }
}
