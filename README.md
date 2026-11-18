# Click Lake Erasure API

面向广告数据分析的点击数据湖服务：按日追加上传点击 CSV、本地 Parquet 存储、按日下载，
并支持 GDPR 风格的用户数据清除（erasure）——受理后立即拦截该用户的新数据，后台重写
受影响文件，完成后永久拦截。

技术栈：**FastAPI + PyArrow + SQLite**（SQLite 作为状态库，Parquet 作为数据湖格式）。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
```

服务监听 `http://localhost:8000`，交互式文档见 `http://localhost:8000/docs`。
数据与状态持久化在命名卷 `clicklake-data` 中。

### 本地开发

```bash
pip install -r requirements-dev.txt
export DATA_DIR=./data STATE_DIR=./state
uvicorn --factory app.main:create_app --reload
```

### 运行测试

```bash
python -m pytest tests/ -q
```

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/upload/{date}` | 上传当日点击 CSV（multipart 文件字段 `file`），返回接收/拦截/入库行数 |
| `GET` | `/dates` | 列出所有日期（文件数、行数、是否受限）；隔离批次不计入 |
| `GET` | `/dates/{date}/download?format=csv\|parquet` | 按日下载（不含隔离批次）；日期受限时返回 `409` |
| `POST` | `/batches/{file_id}/quarantine` | 隔离某次上传的批次，立即从报表取数中剔除 |
| `POST` | `/batches/{file_id}/release` | 放行隔离批次；涉及该批的清除未完成时返回 `409` |
| `GET` | `/batches/{file_id}` | 查询批次状态（`active` / `quarantined`）与当前行数 |
| `POST` | `/erasure` | 提交清除请求 `{"user_id": "..."}`（幂等，重复提交返回同一任务） |
| `GET` | `/erasure/{task_id}` | 查询清除进度 |
| `GET` | `/erasure?user_id=...` | 列出清除任务 |
| `GET` | `/health` | 健康检查 |

### 上传 CSV 格式

表头必须包含 `user_id,page,timestamp`（额外列被忽略）。时间戳支持 ISO-8601
（建议带 `Z` 或时区偏移）与 Unix 秒，统一归一化为 UTC 存储。

```bash
curl -X POST -F "file=@clicks.csv" http://localhost:8000/upload/2026-11-01
# {"date":"2026-11-01","received":4,"invalid":0,"blocked":1,"stored":3,"file_id":"..."}
```

- `received`：CSV 数据行总数；`invalid`：格式非法被跳过的行数
- `blocked`：因清除请求被拦截（未写入）的行数；`stored`：实际入库行数
- `file_id`：本次上传的批次 id（`stored=0` 时为 `null`），后续隔离/放行/查询都用它

### 批次隔离（可疑数据暂停供数）

```bash
FILE_ID=$(curl -s -X POST -F "file=@clicks.csv" http://localhost:8000/upload/2026-11-01 | jq -r .file_id)

curl -X POST http://localhost:8000/batches/$FILE_ID/quarantine
# {"file_id":"...","date":"2026-11-01","state":"quarantined","rows":3,"files":1,...}

curl http://localhost:8000/batches/$FILE_ID      # 查询状态
curl -X POST http://localhost:8000/batches/$FILE_ID/release   # 核实后放行
```

- **按批隔离**：隔离以 `file_id` 为单位，同一天其他批次照常供数；该批立即从按日下载
  和 `/dates` 的行数/文件数中消失。整日都被隔离时，该日下载返回 `404`。
- **已开始的下载可结束**：下载在日期锁内一次性选定文件集合后再读盘，隔离/放行不会让
  一次进行中的下载看到半隔离的日期；隔离立即对之后发起的请求生效（旧链接同样）。
- **隔离参与清除**：隔离只影响供数，不影响 erasure；清除仍会重写/删除该批的文件。
- **file_id 稳定**：清除重写会生成新的文件版本，但版本继承原批次的 `file_id`，因此
  重写后仍可用上传返回的 `file_id` 查询、放行。若整批数据都被清除，该批行数归零，
  放行不会恢复已清除的点击（也拿不到数据）。
- **放行前校验**：若仍有覆盖该批的 erasure 任务未完成（`pending`/`processing`/`failed`），
  放行返回 `409` 并列出 `blocking_tasks`，隔离保持不变；任务完成后才允许放行。
  隔离、放行与清除交错时不会短暂泄露数据。
- **持久化**：批次登记与文件版本登记在同一个 SQLite 事务内完成。登记失败的上传不会
  被供数，其残余 part 文件在重启时作为孤儿清理；重启后隔离状态与供数结果保持一致。

### 清除流程

```bash
curl -X POST -H "Content-Type: application/json" \
     -d '{"user_id":"alice"}' http://localhost:8000/erasure
# 201 Created（重复提交返回 200 与同一 task_id）

curl http://localhost:8000/erasure/<task_id>
# {"status":"processing","files_total":2,"files_done":1,"restricted_dates":["2026-11-01"],...}
```

任务状态机：`pending → processing → completed`，失败时为 `failed`（自动重试，重启后继续）。

## 清除语义（保证）

- **立即拦截**：请求受理的同一事务内，该用户即进入拦截集合；之后提交的上传直接丢弃其记录。
- **在途上传不写回**：上传的"过滤 → 写 Parquet → 提交状态"与"受理清除"在同一把提交锁下
  完成，两种操作的临界区不会交错，因此在途上传不可能把被清除用户的数据写回。
- **下载限制**：含该用户的日期在任务完成前暂停新下载（旧链接同样 `409`）；已开始的下载
  在日期锁保护下读取完数据后再响应，可正常结束；其他日期的导入与下载不受影响。
- **完成标准**：后台逐个重写受影响文件（tmp + fsync + 原子 rename），删除含目标数据的旧
  文件后才标记完成；其他用户记录完整保留。
- **交错安全**：单一后台 worker 顺序处理任务；文件被重写时，其他任务的待处理记录在
  同一事务内被重定向到新文件，多个清除与导入交错不会漏删、复活或丢失他人记录。
  完成前还有一次全量校验扫描作为兜底。
- **失败与重启**：写盘失败时任务进入 `failed` 并自动重试，期间拦截与日期限制保持有效；
  进程重启后恢复所有未完成任务，并清理临时文件、孤儿文件与未完成的删除。
- **幂等与永久拦截**：每个用户终生对应同一任务（`user_id` 唯一约束），重复提交返回同一
  任务；任务行永不删除，因此完成后该用户被永久拦截。

## 数据布局

```
data/
  date=2026-11-01/
    part-<file_id>.parquet      # 每次上传追加一个 part 文件；清除重写生成新版本
state/
  erasure.db                    # SQLite：批次、文件版本、任务、限制日期、用户-日期索引
```

`batches.file_id` 是上传返回的批次 id；`files.batch_id` 把每个文件版本（含清除重写
产生的新版本）归属回原批次。旧版本状态库启动时自动迁移（每个历史文件视为一个批次）。

Parquet schema：`user_id: string, page: string, ts: timestamp[us, tz=UTC]`。

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATA_DIR` | `./data` | Parquet 数据目录 |
| `STATE_DIR` | `./state` | SQLite 状态目录 |
| `ERASURE_RETRY_DELAY` | `5` | 失败任务重试间隔（秒） |
| `ERASURE_WORKER_POLL` | `0.2` | 后台 worker 轮询间隔（秒） |
