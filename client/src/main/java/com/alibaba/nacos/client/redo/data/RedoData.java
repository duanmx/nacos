/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.client.redo.data;

import java.util.Objects;

/**
 * 重做数据基类 —— 客户端注册/注销操作的"重做日志"载体。
 *
 * <h2>核心职责</h2>
 * <p>封装一个需要与服务端最终一致的操作数据及其状态机。
 * 当客户端与服务端断连后，RedoScheduledTask 遍历所有 RedoData，
 * 通过 getRedoType() 判断是否需要重放注册/注销操作。</p>
 *
 * <h2>状态机</h2>
 * <p>两个布尔标志组合出 4 种状态：</p>
 * <table>
 *   <tr><th>registered</th><th>unregistering</th><th>getRedoType()</th><th>含义</th></tr>
 *   <tr><td>true</td><td>false</td><td>NONE / UNREGISTER</td>
 *       <td>已注册成功，如果 expectedRegistered 则无需操作，否则需注销</td></tr>
 *   <tr><td>true</td><td>true</td><td>UNREGISTER</td>
 *       <td>已注册过，现在需要注销（正在注销中）</td></tr>
 *   <tr><td>false</td><td>false</td><td>REGISTER</td>
 *       <td>还未注册成功，需要重新注册</td></tr>
 *   <tr><td>false</td><td>true</td><td>REGISTER / REMOVE</td>
 *       <td>未注册且正在注销中：expectedRegistered=true 则需注册，否则移除数据</td></tr>
 * </table>
 *
 * <h2>RedoType 说明</h2>
 * <ul>
 *   <li><b>REGISTER</b> —— 需要重做注册操作</li>
 *   <li><b>UNREGISTER</b> —— 需要重做注销操作</li>
 *   <li><b>NONE</b> —— 无需任何重做（状态已与服务端一致）</li>
 *   <li><b>REMOVE</b> —— 从 redoDataMap 中移除此数据（已完成注销且不再需要注册）</li>
 * </ul>
 *
 * <h2>子类</h2>
 * <ul>
 *   <li>Naming：InstanceRedoData、BatchInstanceRedoData、SubscriberRedoData、NamingRedoData</li>
 *   <li>Config：ConfigRedoData</li>
 *   <li>AI：AgentEndpointRedoData、McpServerEndpointRedoData</li>
 * </ul>
 *
 * @param <T> 业务数据类型（如 Instance、Subscriber 等）
 * @author xiweng.yy
 */
public abstract class RedoData<T> {
    
    /**
     * 期望的最终状态 —— true 表示期望服务端最终有这条数据（已注册），
     * false 表示期望服务端最终没有这条数据（已注销）。
     */
    private volatile boolean expectedRegistered;
    
    /**
     * 是否已在服务端注册成功 —— true 表示最近一次注册操作成功。
     */
    private volatile boolean registered;
    
    /**
     * 是否正在注销 —— true 表示正在向服务端发送注销请求。
     */
    private volatile boolean unregistering;
    
    private T data;
    
    protected RedoData() {
        this.expectedRegistered = true;
    }
    
    public void setExpectedRegistered(boolean registered) {
        this.expectedRegistered = registered;
    }
    
    public boolean isExpectedRegistered() {
        return expectedRegistered;
    }
    
    public boolean isRegistered() {
        return registered;
    }
    
    public boolean isUnregistering() {
        return unregistering;
    }
    
    public void setRegistered(boolean registered) {
        this.registered = registered;
    }
    
    public void setUnregistering(boolean unregistering) {
        this.unregistering = unregistering;
    }
    
    public T get() {
        return data;
    }
    
    public void set(T data) {
        this.data = data;
    }
    
    public void registered() {
        this.registered = true;
        this.unregistering = false;
    }
    
    public void unregistered() {
        this.registered = false;
        this.unregistering = true;
    }
    
    public boolean isNeedRedo() {
        return !RedoType.NONE.equals(getRedoType());
    }
    
    /**
     * Get redo type for current redo data without expected state.
     *
     * <ul>
     *     <li>{@code registered=true} & {@code unregistering=false} means data has registered, so redo should not do anything.</li>
     *     <li>{@code registered=true} & {@code unregistering=true} means data has registered and now need unregister.</li>
     *     <li>{@code registered=false} & {@code unregistering=false} means not registered yet, need register again.</li>
     *     <li>{@code registered=false} & {@code unregistering=true} means not registered yet and not continue to register.</li>
     * </ul>
     *
     * @return redo type
     */
    public RedoType getRedoType() {
        if (isRegistered() && !isUnregistering()) {
            return expectedRegistered ? RedoType.NONE : RedoType.UNREGISTER;
        } else if (isRegistered() && isUnregistering()) {
            return RedoType.UNREGISTER;
        } else if (!isRegistered() && !isUnregistering()) {
            return RedoType.REGISTER;
        } else {
            return expectedRegistered ? RedoType.REGISTER : RedoType.REMOVE;
        }
    }
    
    public enum RedoType {
        
        /**
         * Redo register.
         */
        REGISTER,
        
        /**
         * Redo unregister.
         */
        UNREGISTER,
        
        /**
         * Redo nothing.
         */
        NONE,
        
        /**
         * Remove redo data.
         */
        REMOVE;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RedoData<?> redoData = (RedoData<?>) o;
        return registered == redoData.registered && unregistering == redoData.unregistering
            && Objects.equals(data,
                redoData.data);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(registered, unregistering, data);
    }
}
