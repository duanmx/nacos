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

package com.alibaba.nacos.common.notify.demo;

import java.util.function.BiFunction;

/**
 * Demo6: Lambda 语法糖 —— 从匿名类到 Lambda 的演变
 *
 * <p>这个 Demo 不涉及 Nacos 任何逻辑，纯粹讲 Java 语法。
 * 运行后你会看到三种写法输出完全一样，因为它们编译后是等价的。
 *
 * @author nacos
 */
public class Demo6LambdaSyntax {

    // ==================== 定义函数式接口 ====================

    /**
     * 这就是 EventPublisherFactory 的简化版
     * 继承 BiFunction，只有一个抽象方法 apply()
     */
    interface MyFactory extends BiFunction<String, Integer, String> {

        /**
         * @param param1 第一个参数（对应 Nacos 中的 eventType）
         * @param param2 第二个参数（对应 Nacos 中的 queueSize）
         * @return 产物（对应 Nacos 中的 EventPublisher）
         */
        @Override
        String apply(String param1, Integer param2);
    }

    // ==================== 第1步：传统匿名类 ====================

    static void traditionalWay() {
        // 传统写法：new 接口 + @Override + 方法体
        MyFactory factory = new MyFactory() {
            @Override
            public String apply(String param1, Integer param2) {
                return "用 [" + param1 + "] 创建了一个产物，大小=" + param2;
            }
        };

        // 调用
        String result = factory.apply("OrderEvent", 16384);
        System.out.println("[传统匿名类] " + result);
    }

    // ==================== 第2步：Lambda 写法 ====================

    static void lambdaWay() {
        // Lambda 写法：省略方法名、参数类型、@Override
        // 编译器知道 MyFactory 只有一个抽象方法 apply(String, Integer)
        // 所以 (a, b) 就自动匹配到 apply 的两个参数
        MyFactory factory = (a, b) -> {
            return "用 [" + a + "] 创建了一个产物，大小=" + b;
        };

        String result = factory.apply("OrderEvent", 16384);
        System.out.println("[Lambda]    " + result);
    }

    // ==================== 第3步：Lambda 简化（单行 return 可省略大括号） ====================

    static void lambdaSimpleWay() {
        // 如果方法体只有一行 return，可以进一步省略 return 和大括号
        MyFactory factory = (a, b) -> "用 [" + a + "] 创建了一个产物，大小=" + b;

        String result = factory.apply("OrderEvent", 16384);
        System.out.println("[Lambda简化] " + result);
    }

    // ==================== 第4步：模拟 Nacos 的闭包捕获 ====================

    /**
     * Nacos 中 clazz 是 static 块中确定的变量
     * Lambda 创建时不执行，但"记住"了 clazz 的值
     * 等 apply() 被调用时才用 clazz
     */
    static String whichImpl = "DefaultPublisher";  // 模拟 clazz

    static MyFactory createFactoryLikeNacos() {
        // 这就是 Nacos 中 DEFAULT_PUBLISHER_FACTORY 的写法
        // Lambda 捕获了外部的 whichImpl 变量
        return (cls, buffer) -> {
            // whichImpl 是外部变量（模拟 Nacos 的 clazz）
            // cls 和 buffer 是调用时传入的参数
            return "用 " + whichImpl + " 处理 [" + cls + "]，队列大小=" + buffer;
        };
    }

    static void nacosLikeWay() {
        // 第1步：创建工厂（Lambda 存着，不执行）
        MyFactory factory = createFactoryLikeNacos();
        System.out.println("[工厂已创建，但还没执行]");

        // 第2步：改变外部变量（模拟 SPI 可能加载不同的实现）
        whichImpl = "CustomPublisher";

        // 第3步：调用工厂（Lambda 此时才执行）
        String result = factory.apply("OrderEvent", 16384);
        System.out.println("[Nacos风格] " + result);
    }

    // ==================== 对比汇总 ====================

    public static void main(String[] args) {

        System.out.println("=== 第1步：传统匿名类（你熟悉的写法） ===");
        traditionalWay();

        System.out.println("\n=== 第2步：Lambda（等价写法） ===");
        lambdaWay();

        System.out.println("\n=== 第3步：Lambda简化（单行省略大括号） ===");
        lambdaSimpleWay();

        System.out.println("\n=== 第4步：Nacos风格（闭包捕获外部变量） ===");
        nacosLikeWay();

        System.out.println("\n=== 总结 ===");
        System.out.println("(a, b) -> { ... } 只是 new 接口() { @Override method(a, b) { ... } } 的语法糖");
        System.out.println("编译后完全一样，字节码都相同");
        System.out.println();
        System.out.println("Nacos 中 NotifyCenter 的写法：");
        System.out.println("  DEFAULT_PUBLISHER_FACTORY = (cls, buffer) -> {");
        System.out.println("      EventPublisher publisher = clazz.newInstance();  // clazz 是闭包捕获的");
        System.out.println("      publisher.init(cls, buffer);                     // cls buffer 是调用时传入的");
        System.out.println("      return publisher;");
        System.out.println("  };");
        System.out.println();
        System.out.println("等价于：");
        System.out.println("  DEFAULT_PUBLISHER_FACTORY = new EventPublisherFactory() {");
        System.out.println("      @Override");
        System.out.println("      public EventPublisher apply(Class cls, Integer buffer) {");
        System.out.println("          EventPublisher publisher = clazz.newInstance();");
        System.out.println("          publisher.init(cls, buffer);");
        System.out.println("          return publisher;");
        System.out.println("      }");
        System.out.println("  };");
    }
}
