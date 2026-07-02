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

/**
 * 元素-权重对 —— 用于 Chooser 加权随机选择算法的输入数据单元。
 *
 * <h2>核心职责</h2>
 * <p>将元素与其权重绑定为一个不可变数据对，由 {@link Chooser} 消费。
 * weight 值越大，randomWithWeight() 中被选中的概率越高。</p>
 *
 * @param <T> 元素类型
 * @author nkorange
 */
public class Pair<T> {
    
    /**
     * 元素本身。
     */
    private final T item;
    
    /**
     * 权重 —— weight=0 的元素会被 Chooser.Ref.refresh() 忽略。
     */
    private final double weight;
    
    public Pair(T item, double weight) {
        this.item = item;
        this.weight = weight;
    }
    
    public T item() {
        return item;
    }
    
    public double weight() {
        return weight;
    }
}
