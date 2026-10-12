# Dameng Connector

基于 Debezium 和 Flink CDC 的达梦数据库变更数据捕获解决方案

## 项目简介

Dameng Connector 是一个专门为达梦数据库(DM Database)设计的变更数据捕获(CDC)解决方案。该项目通过扩展 Debezium 和 Flink CDC，实现了对达梦数据库的实时数据变更监控、捕获和处理能力，为数据集成、数据同步、实时分析等场景提供强大支持。

## 快速开始

### 前置条件

- Java 8+
- 达梦数据库 8.x+
- Maven 3.6+

### 安装配置

1. **克隆仓库**

```bash
git clone https://github.com/RyanWee68/dameng-connector.git
cd dameng-connector
```

2. **编译项目**

```bash
./mvnw clean package -DskipTests
```

### 使用示例

#### 1. 独立模式运行 Debezium

```bash
# 启动 Debezium 独立服务器
debezium-server/run.sh

# 注册达梦连接器
curl -X POST -H "Content-Type: application/json" --data @dm-connector.json http://localhost:8083/connectors
```

## 配置选项

### Debezium 连接器配置

| 选项                   | 描述       | 默认值                                  | 必填 |
|----------------------|----------|--------------------------------------|----|
| connector.class      | 连接器类名    | org.devlive.connector.dameng.DamengConnector | 是  |
| database.hostname    | 达梦数据库主机名 | -                                    | 是  |
| database.port        | 达梦数据库端口号 | 5236                                 | 是  |
| database.user        | 数据库用户名   | -                                    | 是  |
| database.password    | 数据库密码    | -                                    | 是  |
| database.dbname      | 数据库名称    | -                                    | 是  |
| database.server.name | 连接器唯一名称  | -                                    | 是  |
| table.include.list   | 需要捕获的表列表 | -                                    | 否  |
| snapshot.mode        | 快照模式     | initial                              | 否  |
| transforms           | 转换配置     | -                                    | 否  |
| debezium.source.transaction.auto.commit.timeout.ms | 未提交事务的静默超时，单位毫秒；0 禁用自动提交 | 0 | 否 |
| log.mining.transaction.retention.hours | 长事务缓冲保留期限，单位小时；0 不主动丢弃 | 0 | 否 |

### 事务提交与大事务捕获

连接器默认等待数据库真实的 `COMMIT` 后才发送缓冲区中的 DML；收到 `ROLLBACK` 时丢弃对应变更。未提交事务即使长时间没有新 DML，也不会因超时被发送。

在应用的 properties 文件中设置，并将属性传给连接器：

```properties
debezium.source.transaction.auto.commit.timeout.ms=0
```

使用 Flink DataStream Builder 时通过 `.property("debezium.source.transaction.auto.commit.timeout.ms", "0")` 传入。升级前默认值为 `1000` 毫秒；已有配置若显式指定了正数，升级后仍会启用超时自动提交，需要改为 `0` 才能等待真实 COMMIT。

**正数超时有一致性风险**：该时间从连接器最后一次读取该事务 DML 时计算，无法证明数据库已经提交。超时发送的变更在数据库随后回滚时无法撤回。要求只同步已提交数据时，应保持为 `0`。

严格等待 COMMIT 时，长事务会占用缓冲内存，并可能阻止恢复位点前进；应保证数据库日志保留时间覆盖事务持续时间和消费延迟。若 LogMiner 未返回 COMMIT，该事务会继续等待。将 `log.mining.transaction.retention.hours` 设为正数可能主动丢弃长事务，造成其变更缺失。

LogMiner 查询按 `SCN, RS_ID, SSN` 排序，确保按日志顺序处理 DML、SQL 续片和事务结束事件，避免 COMMIT 提前于尚未处理的 DML 清空缓冲区。另已修复自动提交路径中事务末尾 SCN 未随 DML 更新的问题。

提交时会打印事务号、缓冲区中的 DML 条数和提交 SCN，例如：

```text
Committing transaction ... with 3468 buffered DML events at SCN ...
```

已在测试库验证单次 `INSERT INTO ... SELECT ...` 影响 3,468 行时，Sink 接收条数为 3,468。核对其他场景时应比较本次操作前后的 Sink 接收条数增量；最大编码不能证明没有重复或中间缺号。回滚、重启恢复和更大事务仍需按实际环境验证。

## 版本兼容性

| 本项目版本    | Debezium 版本 | 达梦数据库版本 |
|----------|-------------|---------|
| 2025.1.0 | 1.9.x       | 8.x     |
| 2026.1.0 | 1.9.8.Final | 8.x     |

## 贡献指南

我们非常欢迎社区贡献！如果您想参与项目开发，请遵循以下步骤：

1. Fork 本仓库
2. 创建您的特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交您的更改 (`git commit -m 'Add some amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 开启一个 Pull Request

## 许可证

本项目采用 MIT 许可证。详情请参阅 [LICENSE](LICENSE) 文件。

## 联系方式

- 项目维护者：您的姓名
- 电子邮件：support@devlive.org
- 项目问题跟踪：[GitHub Issues](https://github.com/devlive-community/dameng-connector/issues)

---

# 达梦数据库 CDC 配置指南

本文档介绍如何为达梦数据库配置变更数据捕获(CDC)功能，以便与 Debezium 等工具集成。

## 1. 准备测试环境

首先，需要创建测试数据库和表，并插入一些测试数据：

```sql
-- 测试数据库
CREATE SCHEMA TEST;

-- 测试数据表
CREATE TABLE "TEST"."T00003"
(
    "ID" NUMBER PRIMARY KEY,
    "NAME" VARCHAR(8188),
    "AGE" NUMBER
);

-- 测试数据
INSERT INTO "TEST"."T00003" ("ID", "NAME", "AGE") VALUES (1, '张三', 25);
INSERT INTO "TEST"."T00003" ("ID", "NAME", "AGE") VALUES (2, '李四', 30);
INSERT INTO "TEST"."T00003" ("ID", "NAME", "AGE") VALUES (3, '王五', 28);
INSERT INTO "TEST"."T00003" ("ID", "NAME", "AGE") VALUES (4, '赵六', 35);
INSERT INTO "TEST"."T00003" ("ID", "NAME", "AGE") VALUES (5, '钱七', 22);
INSERT INTO "TEST"."T00003" ("ID", "NAME", "AGE") VALUES (6, '孙八', 40);
INSERT INTO "TEST"."T00003" ("ID", "NAME", "AGE") VALUES (7, '周九', 29);
INSERT INTO "TEST"."T00003" ("ID", "NAME", "AGE") VALUES (8, '吴十', 31);
INSERT INTO "TEST"."T00003" ("ID", "NAME", "AGE") VALUES (9, '刘十一', 27);
INSERT INTO "TEST"."T00003" ("ID", "NAME", "AGE") VALUES (10, '陈十二', 33);
```

## 2. 配置兼容模式

需要开启兼容模式，以确保 CDC 功能正常工作：

```sql
-- 开启兼容模式，需要重启服务器，或者修改 dm.ini
SP_SET_PARA_VALUE(2,'compatible_mode',2);
SELECT para_name,para_type,para_value FROM V$DM_INI WHERE PARA_NAME = 'COMPATIBLE_MODE';
```

**注意**：修改兼容模式后，需要重启数据库服务才能生效。您也可以直接在 dm.ini 文件中修改配置项。

## 3. 开启归档日志

CDC 功能依赖于数据库的归档日志模式：

```sql
-- 开启归档
ALTER DATABASE MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE ADD ARCHIVELOG 'DEST = /data/dameng/data/DAMENG/archivelog, TYPE = local, FILE_SIZE = 1024, SPACE_LIMIT = 2048';
ALTER DATABASE OPEN;
```

## 4. 开启闪回功能

配置闪回功能以支持 CDC 操作：

```sql
-- 设置回闪
SP_SET_PARA_VALUE(1,'ENABLE_FLASHBACK',1);
SELECT para_name,para_value FROM V$DM_INI WHERE PARA_NAME ='ENABLE_FLASHBACK';
```

## 5. 开启日志追加模式（关键步骤）

**重要**：此步骤是确保 CDC 正常工作的关键，如果不配置，`V$LOGMNR_CONTENTS` 视图可能始终为空，导致无法捕获数据变更。

```sql
-- 注意：这里一定要处理，设置为 1 否则 V$LOGMNR_CONTENTS 一直是 0 导致获取不到数据
-- 开启日志追加：修改 dm.ini 配置项 RLOG_APPEND_LOGIC=1 重启服务

-- 查询是否是追加模式
SELECT para_name, para_value FROM v$dm_ini WHERE para_name = 'RLOG_APPEND_LOGIC';
```

您需要直接修改 dm.ini 配置文件，添加或修改以下配置项：

```
RLOG_APPEND_LOGIC=1
```

**注意**：修改 dm.ini 后需要重启数据库服务才能生效。

## 6. 数据库服务启动

使用后台方式启动数据库服务（生产环境推荐）：

```bash
nohup ./dmserver /data/dameng/data/DAMENG/dm.ini > /path/to/logfile.log 2>&1 &
```

启动后，可以通过以下命令确认进程是否在运行：

```bash
ps -ef | grep dmserver
```

## 7. 验证配置

完成上述配置后，您可以尝试在表中插入新数据，然后观察 Debezium 连接器的日志。如果配置正确，应该能够在日志中看到类似以下内容的输出：

```
[debezium-damengconnector-my-dameng-connector-2025-0226-change-event-source-coordinator] INFO io.debezium.connector.dameng.logminer.LogMinerQueryResultProcessor - 1 Rows, 0 DMLs, 0 Commits, 0 Rollbacks, 0 Inserts, 0 Updates, 0 Deletes. Processed in 0 millis. Lag:0. Offset scn:50684. Offset commit scn:null. Active transactions:0. Sleep time:3000
```

如果看到 `0 Rows` 的输出，请检查是否正确配置了所有步骤，特别是日志追加模式设置。

## 8. 常见问题排查

如果遇到问题，可以检查以下几点：

1. 确认兼容模式设置为 2
2. 确认数据库处于归档日志模式
3. 确认闪回功能已开启
4. 确认 RLOG_APPEND_LOGIC 设置为 1
5. 确认所有需要重启服务的配置都已生效

*备注：本项目不是达梦数据库官方项目，由社区维护和开发。*
