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

LogMiner 查询按 `SCN, RS_ID, SSN` 排序，确保按日志顺序处理 DML、SQL 续片和事务结束事件，避免 COMMIT 提前于尚未处理的 DML 清空缓冲区。另已修复自动提交路径中事务末尾 SCN 未随 DML 更新的问题。

需要诊断事务提交时，可将 `org.devlive.connector.dameng.logminer.TransactionalBuffer` 日志级别设为 DEBUG，查看事务号、缓冲 DML 条数和提交 SCN，例如：

```text
Committing transaction ... with 3468 buffered DML events at SCN ...
```

### 自适应扫描窗口

扫描窗口单位是 SCN 跨度，不是业务行数或日志条数。正式版本采用积压时倍增、接近数据库当前位置时线性回落的策略。默认初始值 20,000、最小值及缩窗步长 1,000、最大值 100,000。以下是本次测试使用的配置；应用需将属性传入连接器，Flink DataStream Builder 使用 `.property(key, value)`：

```properties
log.mining.batch.size.default=20000
log.mining.batch.size.min=5000
log.mining.batch.size.max=100000
```

`min` 同时控制最小窗口和缩窗步长，不再控制扩窗步长。配置必须满足 `0 < min <= default <= max`。当数据库当前 SCN 超过本轮窗口终点的差距大于 `default` 时，下一轮窗口倍增，例如 `20000 → 40000 → 80000 → 100000`；当本轮计划终点超过数据库当前 SCN 的差距大于 `default` 时，下一轮按 `min` 缩窗。窗口达到最大值后保持上限，超过上限的事务继续分轮读取，并跨轮缓存至真实 COMMIT。

正式版本已移除本轮临时的逐阶段、逐轮汇总、解析计时、事务发送计时、文件候选和 JDBC 调用日志，也不执行诊断用 `COUNT(*)` 查询。保留原有指标、错误、告警及必要运行日志；事务提交详情仅在 DEBUG 输出。独立的 dm_test 是测试程序，不属于本仓库的发布产物。

启动挖掘继续使用 [达梦官方支持的 STARTSCN / ENDSCN](https://eco.dameng.com/document/dm/zh-cn/pm/dbms_logmnr-package) 限制分析或加载范围；正式读取仍为 `SCN > startScn AND SCN <= endScn`。不改变排序、会话重建或真实 COMMIT 提交规则，不启用数据库侧 COMMITTED_DATA_ONLY。

### 可行性与风险

项目仍存在大事务容量、性能及数据一致性风险，详细说明、测试结果与验收建议见 [可行性与风险记录](docs/feasibility-and-risks.md)。

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

## 2. 参数配置范围与兼容模式

本次测试反馈修改了以下 `dm.ini` 参数，最终以测试实例的有效值为准：

```ini
ARCH_INI = 1
RLOG_APPEND_LOGIC = 2
LOGMNR_PARSE_LOB = 1
```

| 参数 | 作用 | 注意事项 |
|---|---|---|
| `ARCH_INI=1` | 启用归档配置 | 还需有效的 `dmarch.ini` 归档目的地，并确认数据库处于归档模式。 |
| `RLOG_APPEND_LOGIC=2` | 记录逻辑操作；UPDATE、DELETE 不论是否有主键都包含全列信息 | 相比模式 1，可能增加日志量、归档空间和写入 I/O。 |
| `LOGMNR_PARSE_LOB=1` | 支持 LogMiner 挖掘行外 BLOB、CLOB 大字段逻辑日志 | 有大字段时需评估解析、传输和程序内存开销，并验证字段内容完整性。 |

只修改这三项不代表实例没有其他既有配置。还需核实 `RLOG_IGNORE_TABLE_SET`：值为 1 时记录所有表的逻辑日志；值为 0 时，需对捕获表执行 `ALTER TABLE "模式名"."表名" ADD LOGIC LOG`。

`COMPATIBLE_MODE=2` 为 Oracle 兼容模式，未确认本次测试是否修改，不能作为本次已开启项或所有部署的必改项。先查询现有值，仅在目标版本联调确认需要时再调整：

```sql
SELECT para_name,para_type,para_value FROM V$DM_INI WHERE PARA_NAME = 'COMPATIBLE_MODE';
-- 仅在确认需要 Oracle 兼容模式后执行，写入配置文件，重启后生效
-- SP_SET_PARA_VALUE(2,'COMPATIBLE_MODE',2);
```

**注意**：兼容模式为静态参数，修改后需重启，可能影响现有 SQL、数据类型及操作结果。生产变更前应完成业务回归，并准备维护窗口及回退方案。

## 3. 开启归档日志

CDC 功能依赖于数据库的归档日志模式：

先查询 `SELECT * FROM V$DM_ARCH_INI` 和 `SELECT ARCH_MODE FROM V$DATABASE`。以下示例用于尚未开启归档的数据库，已有归档目的地不要重复添加。`MOUNT` 会影响业务访问，生产操作需安排维护窗口。

```sql
-- 开启归档
ALTER DATABASE MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE ADD ARCHIVELOG 'DEST = /data/dameng/data/DAMENG/archivelog, TYPE = local, FILE_SIZE = 1024, SPACE_LIMIT = 2048';
ALTER DATABASE OPEN;
```

示例中的 `FILE_SIZE=1024` 为单文件上限，`SPACE_LIMIT=2048` 为总容量上限，单位均为 MB。约 2 GB 的总上限不是生产推荐值：容量不足可能自动清理 CDC 恢复仍需使用的旧归档；设为 0 不限制容量，又可能写满磁盘。应按开启逻辑日志后的实际生成速率、长事务及故障恢复保留时间规划容量和告警。

## 4. 闪回功能（按初始化方式选择）

本次只做增量同步，不读取已有历史行，可使用 `snapshot.mode=schema_only`，无需为历史数据初始化额外开启 `ENABLE_FLASHBACK`。结构采集和起始位置初始化仍需执行，不能据此认为启动过程完全无锁。

如果以后采用历史数据初始化，连接器默认 `snapshot.flashback.enabled=true`，会使用 `AS OF SCN` 查询，需确认数据库闪回能力及历史保留满足要求。确需开启时可执行：

```sql
-- 仅在使用闪回历史查询的方案中执行
SP_SET_PARA_VALUE(1,'ENABLE_FLASHBACK',1);
SELECT para_name,para_value FROM V$DM_INI WHERE PARA_NAME ='ENABLE_FLASHBACK';
```

也可配置 `snapshot.flashback.enabled=false` 使用普通 SELECT，但这不自动保证历史快照与增量的一致性，也不会消除代码中的显式表锁。不能仅靠调整 JDBC 事务模式就承诺无锁且一致的历史初始化。

## 5. 逻辑日志与大字段解析（关键步骤）

LogMiner 需要可挖掘的逻辑操作日志。本次反馈使用 `RLOG_APPEND_LOGIC=2`，不应写成“必须为 1”：模式 1 在有主键时 UPDATE、DELETE 只包含主键列信息，模式 2 则包含全列信息。参数变化只影响后续生成的相关日志，不会补齐旧日志缺失的字段。

`LOGMNR_PARSE_LOB=1` 用于行外大字段解析。关闭时，在使用在线字典拼写 SQL 的场景下，BLOB、CLOB 内容可能以 `OUT_BLOB`、`OUT_CLOB` 占位；开启后仍需验证连接器到 Sink 的完整字段处理。

```sql
-- 当前官方手册中两项均为动态系统参数，scope=1 修改内存及配置文件
SP_SET_PARA_VALUE(1,'RLOG_APPEND_LOGIC',2);
SP_SET_PARA_VALUE(1,'LOGMNR_PARSE_LOB',1);
```

也可直接修改 `dm.ini` 中对应配置，再按重启流程加载。静态参数和动态参数的生效方式应按实际达梦版本确认；修改文件后不能仅凭文件内容判断运行实例已生效。

```sql
-- 对照当前参数与配置文件值，另核实归档模式和目的地
SELECT PARA_NAME, PARA_VALUE, FILE_VALUE
FROM V$DM_INI
WHERE PARA_NAME IN (
    'ARCH_INI', 'RLOG_APPEND_LOGIC', 'LOGMNR_PARSE_LOB',
    'RLOG_IGNORE_TABLE_SET', 'COMPATIBLE_MODE', 'ENABLE_FLASHBACK'
);
```

参数说明依据：[达梦物理存储及参数说明](https://eco.dameng.com/document/dm/zh-cn/pm/physical-storage)、[DBMS_LOGMNR 包](https://eco.dameng.com/document/dm/zh-cn/pm/dbms_logmnr-package)。

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

空闲时出现 `0 Rows` 不一定异常，上述 `0 DMLs` 日志也不能证明已捕获业务变更。应执行并提交 INSERT、UPDATE、DELETE，核对目标端事件及字段；有大字段时增加 BLOB、CLOB 内容检查。若提交后仍持续无变化，再排查归档、逻辑日志范围、起始位置和表过滤配置。

## 8. 常见问题排查

如果遇到问题，可以检查以下几点：

1. 确认归档配置已启用、数据库处于归档模式，目的地有效且空间充足。
2. 核实本次使用的 `RLOG_APPEND_LOGIC=2`，以及全库或表级逻辑日志范围已覆盖捕获表。
3. 捕获行外大字段时核实 `LOGMNR_PARSE_LOB=1`，并检查实际字段内容。
4. 仅增量方案无需为历史初始化开启闪回；使用 `AS OF SCN` 初始化时才核实相应能力和保留时间。
5. 兼容模式按现有值和联调结果判断，不直接要求改为 2；确认所有参数已按其生效方式加载。
6. 确认业务事务已提交、起始位置及表过滤正确，恢复所需归档未被提前清理。

*备注：本项目不是达梦数据库官方项目，由社区维护和开发。*
