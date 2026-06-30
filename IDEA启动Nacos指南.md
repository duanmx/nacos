# IDEA 启动 Nacos 指南

## 1. 启动入口

**主类**：`com.alibaba.nacos.bootstrap.NacosBootstrap`

位于模块 `bootstrap`，而非 `server` 模块中的 `NacosServerWebApplication`。因为 Nacos v3 采用了**多 Spring 上下文架构**（Core → Web → Console），`NacosBootstrap` 负责按顺序编排启动。

## 2. VM Options（完整）

在 IDEA 的 Run Configuration 中添加以下 VM options：

```
-Dnacos.standalone=true
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
-Dnacos.core.auth.server.identity.key=nacos
-Dnacos.core.auth.server.identity.value=nacos
-Dnacos.core.auth.plugin.nacos.token.secret.key=VGhpc0lzTXlDdXN0b21TZWNyZXRLZXkwMTIzNDU2Nzg=
```

## 3. 参数说明

| 参数 | 作用 | 缺失时的错误 |
|------|------|-------------|
| `-Dnacos.standalone=true` | 启用单机模式，使用内嵌 Derby 数据库 | `IllegalArgumentException: db.num is null` — 系统误判为集群模式，要求外部数据库配置 |
| `--add-opens java.base/java.lang=ALL-UNNAMED` | JDK 17+ 模块系统反射开放 | `InaccessibleObjectException` — JRaft 框架反射访问 `java.base` 内部 API 被拒绝 |
| `--add-opens java.base/java.lang.reflect=ALL-UNNAMED` | 同上 | 同上 |
| `--add-opens java.base/java.util=ALL-UNNAMED` | 同上 | `InaccessibleObjectException: Unable to make field transient java.lang.Object[] java.util.ArrayList.elementData accessible` |
| `--add-opens java.base/java.nio=ALL-UNNAMED` | 同上 | 同上 |
| `--add-opens java.base/sun.nio.ch=ALL-UNNAMED` | 同上 | 同上 |
| `-Dnacos.core.auth.server.identity.key` | 服务间认证身份凭证 | `NacosRuntimeException: Empty identity` |
| `-Dnacos.core.auth.server.identity.value` | 服务间认证身份凭证 | 同上 |
| `-Dnacos.core.auth.plugin.nacos.token.secret.key` | JWT Token 签名密钥（原串 ≥32 字符后 Base64 编码） | `IllegalArgumentException: the length of secret key must great than or equal 32 bytes; And the secret key must be encoded by base64` |

## 4. 常见错误速查

| 错误信息关键词 | 原因 | 解决 |
|--------------|------|------|
| `Nacos don't start up` | 运行了错误的 main class | 改为运行 `NacosBootstrap` |
| `db.num is null` | 未设置 standalone 模式 | 添加 `-Dnacos.standalone=true` |
| `InaccessibleObjectException` / `module java.base does not "opens"` | JDK 17+ 模块封装 | 添加 `--add-opens` 参数 |
| `Empty identity` | 未配置服务认证 | 添加 `identity.key` / `identity.value` |
| `secret key must great than or equal 32 bytes` | 未配置 JWT 密钥 | 添加 `token.secret.key`（≥32 字符的 Base64） |

## 5. 启动成功标志

日志中依次出现：

```
Nacos started successfully in CORE mode
Nacos started successfully in WEB mode
Nacos started successfully in CONSOLE mode
```

## 6. 访问地址

- **Nacos 服务端 API**：`http://localhost:8848/nacos`
- **新版控制台 UI**：`http://localhost:8080/next`
- **旧版控制台 UI**：`http://localhost:8080/legacy`
- **默认登录账号**：`nacos` / `nacos`
