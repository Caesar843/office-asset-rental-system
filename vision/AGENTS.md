# AGENTS.md

## 1. 文件作用

本文件用于约束 Codex 在 `vision/` 目录中的所有编码行为，确保实现严格服从本项目已冻结的视觉模块文档，而不是自行扩展职责、改写字段语义、混入后端事务逻辑或重新发明接口契约。

Codex 在 `vision/` 下进行任何新增、修改、重构、补测时，都必须首先遵守本文件。

---

## 2. 最高优先级原则

### 2.1 必须遵守的单一事实源

`vision/` 的正式实现必须以以下文档为唯一依据：

1. 《视觉模块边界定义》
2. 《视觉模块内部契约与核心对象设计书》
3. 《视觉模块对外接口与数据传输契约设计书》
4. 《视觉模块异常处理与去重策略设计书》
5. 《视觉模块配置与阈值约束设计书》
6. 《视觉模块测试与验收设计书》

若代码现状、临时想法、自动补全建议与上述文档冲突，**以文档为准**，不得以“更方便实现”为理由偏离。

### 2.2 不允许擅自扩展正式范围

当前 `vision` 模块的 V1 正式边界固定为：

**采集 → 识别 → 标准化 → 提交**

不得在 `vision/` 内擅自扩展以下职责：

- Borrow / Return / Inbound 事务状态机
- 规则裁决
- 串口协议封包、拆包、ACK、重传、心跳
- MCU 硬件确认逻辑
- 数据库写入与原子提交
- WebSocket 推送
- 看板统计
- 前端页面逻辑

如果任务需要这些职责，必须明确拒绝在 `vision/` 中实现，并指出它们属于后端、串口层、硬件层或前端层。

### 2.3 不允许绕过正式接口

当前 `vision` 模块唯一正式下游接口固定为：

`POST /scan/result`

不得擅自新增或长期并行使用以下等价接口：

- `/scan`
- `/vision/result`
- `/recognition/upload`

不得直连数据库，不得直连串口程序，不得直连 MCU。

---

## 3. 目录级职责约束

`vision/` 目录结构的职责边界固定如下，禁止跨层污染：

### 3.1 `capture/`
只负责视频源接入与取帧。

允许：
- webcam / ip_camera / image_file / video_file 取流
- 输出统一 `FrameData`

禁止：
- 直接调用后端接口
- 做业务规则判断
- 拼装 `ScanSubmitRequest`
- 写日志以外的业务结果收口

### 3.2 `preprocess/`
只负责视觉前处理与质量门禁。

允许：
- 灰度化
- 对比度增强
- ROI 裁剪
- 清晰度检测
- 一次轻量增强重试

禁止：
- 直接解析 `asset_id`
- 构造后端请求
- 写事务字段
- 自行决定借还业务是否通过

### 3.3 `decoder/`
只负责二维码/条码原始解码。

允许：
- 输出 `DecodeResult`
- 支持 QR / Barcode / Hybrid decoder

禁止：
- 直接输出事务对象
- 跳过 parser 直接生成 `ScanSubmitRequest`
- 混入后端字段

### 3.4 `parser/`
只负责：
- `asset_id` 提取
- 标准化
- 去重判定

允许：
- `asset_id_parser.py`
- `normalizer.py`
- `deduplicator.py`

禁止：
- 直接发 HTTP
- 写数据库
- 创建 Borrow/Return 请求
- 生成 `request_seq / request_id / hw_seq / hw_result`

### 3.5 `gateway/`
只负责与后端扫描结果接口交互。

允许：
- 接收 `ScanSubmitRequest`
- 调用 `POST /scan/result`
- 解析 `ScanSubmitResponse`
- 收口传输失败

禁止：
- 夹带事务逻辑
- 直连数据库
- 自创第二套接口路径
- 伪造后端事务字段

### 3.6 `app/`
只负责：
- 配置加载
- pipeline 编排
- runner / main 启动流程

禁止：
- 在 `app/` 中写具体 decoder 细节
- 在 `app/` 中写 parser 细节
- 在 `app/` 中硬编码配置常量
- 在 `app/` 中实现 Borrow/Return 业务逻辑

### 3.7 `models/`
只定义视觉模块正式对象。

禁止：
- 定义后端事务对象
- 定义数据库 ORM
- 定义串口协议帧对象
- 定义 MCU 事件对象

### 3.8 `tests/`
只写视觉模块正式测试。

必须覆盖：
- `test_decoder.py`
- `test_parser.py`
- `test_pipeline.py`
- `test_api_client.py`
- `test_config.py`

---

## 4. 正式对象与字段约束

### 4.1 只允许使用的正式核心对象

当前 V1 正式核心对象固定为：

- `FrameData`
- `DecodeResult`
- `ScanResult`
- `ScanSubmitRequest`
- `VisionErrorResult`
- `VisionConfig`

除非文档明确新增，否则不得擅自发明平行正式对象替代它们。

### 4.2 正式对象流转必须保持单向

正式流转链固定为：

`FrameData → DecodeResult → ScanResult → ScanSubmitRequest`

失败链统一收口为：

`VisionErrorResult`

不得出现以下越级跳转：

- `FrameData → ScanSubmitRequest`
- `DecodeResult → HTTP 请求`
- `ScanResult → 数据库写入`
- `DecodeResult → Borrow/Return 事务对象`

### 4.3 正式字段命名必须保持单一事实源

视觉模块内部与对外传输只允许使用以下正式字段名：

- `frame_id`
- `image`
- `timestamp`
- `source_id`
- `raw_text`
- `symbology`
- `bbox`
- `confidence`
- `asset_id`
- `frame_time`

不得长期并行使用这些平行命名：

- `camera_name`
- `capture_time`
- `scan_code`
- `qr_text`
- `barcode_text`
- `asset_code`
- `code_type`

### 4.4 严格禁止进入视觉正式主语义的字段

以下字段不得进入 `FrameData / DecodeResult / ScanResult / ScanSubmitRequest` 的正式职责字段：

- `request_seq`
- `request_id`
- `hw_seq`
- `hw_result`
- `transaction_state`
- `device_status`

在当前 V1 的纯扫描结果上报模式下，以下字段也不纳入 `ScanSubmitRequest`：

- `user_id`
- `action_type`

如果任务要求这些字段，必须明确指出：它们属于后端业务入口阶段，不属于当前 `vision` 正式对象。

---

## 5. 对外接口约束

### 5.1 唯一正式提交对象

向后端提交时只能提交：

`ScanSubmitRequest`

其正式主字段固定为：

- `asset_id`
- `raw_text`
- `symbology`
- `source_id`
- `frame_time`

可选字段为：

- `frame_id`
- `confidence`
- `bbox`
- `extra`

### 5.2 `frame_time` 约束

`frame_time` 在 V1 阶段必须统一为：

**Unix 时间戳整数（秒）**

禁止：
- 毫秒整数
- 浮点秒
- 字符串时间
- HTTP 发送时刻覆盖采集时刻

### 5.3 传输协议约束

正式提交方式固定为：

- HTTP
- POST
- JSON

不引入：

- 表单上传
- WebSocket 主动提交
- 直连数据库
- 直连串口网关

### 5.4 响应判定约束

HTTP 2xx 不等于扫描业务成功。

业务是否成功必须以响应体中的以下字段为准：

- `success`
- `code`
- `message`

禁止把“HTTP 成功”误判为“业务成功”。

---

## 6. 异常处理与去重约束

### 6.1 失败统一对象

视觉模块内部正式失败表达对象固定为：

`VisionErrorResult`

不得长期混用以下失败表达方式：

- 裸字符串
- `None`
- 随意抛异常但不上收口
- 无结构 dict

### 6.2 必须前置拦截的异常

以下异常必须在 `vision/` 内部前置拦截，不得灌入后端：

- 输入源无法打开
- 读帧失败
- 空帧
- 图像质量过低
- 无码
- 空码
- 非法码制
- `asset_id` 解析失败
- 多码冲突
- 重复识别
- 提交前字段不合法

### 6.3 多码冲突判定层级

“多码冲突”必须在**最终 `asset_id` 层**判定，而不是在 `raw_text` 层判定。

规则：

- 多个不同 `raw_text` 最终归一成同一 `asset_id`：不算冲突
- 多个结果最终归一成多个不同正式 `asset_id`：才算 `MULTI_RESULT_CONFLICT`

### 6.4 去重规则

当前 V1 正式去重规则固定为：

- 同帧重复：`same_frame_same_asset`
- 时间窗重复：`within_dedup_window`

时间窗去重主键固定为：

`(asset_id, source_id)`

时间基准固定为：

`frame_time`

默认窗口固定为：

`2 秒`

### 6.5 重复结果处理规则

`ScanResult.is_duplicate == True` 的结果：

- 默认不得映射为 `ScanSubmitRequest`
- 默认不得发起 HTTP 提交
- 允许写日志
- 允许保留调试信息

禁止把重复识别伪装成后端 `BUSY`、`STATE_INVALID` 或其他事务失败码。

---

## 7. 配置约束

### 7.1 唯一正式配置入口

配置唯一正式入口固定为：

`vision/app/config.py`

禁止：
- 各模块私自维护本地常量
- `pipeline.py` 内散乱常量
- `api_client.py` 自带独立后端地址常量
- `deduplicator.py` 自带独立去重窗口常量

### 7.2 顶层配置对象固定为

`VisionConfig`

其正式子配置固定为：

- `CaptureConfig`
- `PreprocessConfig`
- `DecodeConfig`
- `DedupConfig`
- `GatewayConfig`
- `RuntimeConfig`

### 7.3 配置加载与冻结规则

配置必须遵循以下规则：

1. 启动阶段一次性构造完成
2. 构造成功后作为本次运行唯一正式配置对象
3. 运行中不得随意变更对象结构
4. 业务模块不得自行增删配置字段
5. 需要变更配置时，应重新构造并重启流程

### 7.4 非法配置处理规则

- 缺省配置：允许回落默认值
- 非法配置：必须拒绝启动

例如：

- `fps_limit` 未设置：可回落默认值
- `fps_limit = 0`：非法，必须拒绝启动

### 7.5 契约冻结字段

以下内容属于契约冻结项，不得作为普通运行调优项随意改写：

- `/scan/result` 为唯一正式入口
- `frame_time` 为 Unix 时间戳整数（秒）
- 去重主键为 `(asset_id, source_id)`
- 去重时间基准为 `frame_time`
- 默认去重窗口为 `2 秒`

尤其：

`scan_result_path` 虽位于 `GatewayConfig` 中，但在当前 V1 中是**配置对象中的契约冻结字段**，不是普通运行调优项。

---

## 8. 测试约束

### 8.1 编码时必须同步补测试

在 `vision/` 下进行正式编码时，不允许只改实现、不补测试。

### 8.2 正式测试文件固定为

- `tests/test_decoder.py`
- `tests/test_parser.py`
- `tests/test_pipeline.py`
- `tests/test_api_client.py`
- `tests/test_config.py`

其中：

`tests/test_config.py` 是正式新增测试文件，不允许把配置测试长期散落并入其他测试文件替代。

### 8.3 必测场景

至少必须覆盖：

#### decoder / parser
- 二维码成功识别
- 条形码成功识别
- 无码
- 空码
- `asset_id` 解析失败
- 多码同资产归一
- 多资产正式冲突

#### deduplicator / pipeline
- 同帧重复
- 时间窗重复
- 重复结果不上报
- 低质量图像拦截
- 成功链输出 `ScanSubmitRequest`
- 失败链输出 `VisionErrorResult`

#### api_client
- 合法请求构造
- 非法请求拦截
- 连接失败
- 请求超时
- 非 JSON 响应
- `success=false` 响应

#### config
- 缺省配置回落默认值
- 非法配置拒绝启动
- 冻结契约字段被篡改时拒绝启动

### 8.4 测试通过口径

不得用以下口径自我宣称“通过”：

- “代码能跑”
- “手工看起来没问题”
- “没有报错”
- “主流程大概通了”

正式通过证据必须是：

- 字段断言
- 错误码断言
- 请求/响应结构断言
- 阶段顺序断言
- 输出类型断言
- 启动成功 / 启动拒绝断言

---

## 9. Codex 的明确禁止行为

Codex 在 `vision/` 编码时，禁止出现以下行为：

1. 未经文档允许，擅自新增 Borrow/Return/Inbound 事务逻辑  
2. 在 `vision/` 内直接操作数据库  
3. 在 `vision/` 内直接接串口、处理 ACK、处理 MCU 事件  
4. 擅自发明第二套接口路径  
5. 擅自引入平行字段命名  
6. 擅自把 `user_id / action_type / request_seq / hw_seq` 混进扫描提交对象  
7. 跳过 `parser`，直接把原始解码结果提交给后端  
8. 跳过去重与异常拦截，直接全量上报  
9. 在实现中硬编码后端地址、去重窗口、质量阈值、日志等级  
10. 只改代码不补测试  
11. 为了“方便”修改既有正式契约  
12. 在没有明确要求的情况下，把 vision 做成通用目标检测平台或多目标跟踪系统

---

## 10. Codex 的执行要求

每次在 `vision/` 内执行编码任务时，必须遵循以下顺序：

1. 先确认修改目标属于 `vision` 正式边界  
2. 先确认要改的是哪个目录层  
3. 先确认会影响哪些正式对象和字段  
4. 先确认是否触及 `/scan/result` 契约  
5. 先确认是否触及异常、去重或配置冻结规则  
6. 再开始编码  
7. 编码完成后补齐对应测试  
8. 输出结果时，必须说明：
   - 改了哪些文件
   - 是否新增了文件
   - 是否影响正式对象/字段
   - 是否影响接口契约
   - 新增或修改了哪些测试
   - 是否存在未完成项 / 风险点

---

## 11. 默认实现倾向

在没有额外指令时，Codex 应遵循以下默认倾向：

- 优先保证契约正确，再追求“聪明”
- 优先小步实现，再做扩展
- 优先保持目录分层清晰，不跨层偷懒
- 优先保留日志与测试可追溯性
- 优先 mock / stub 后端交互，不伪造业务闭环成功
- 优先按 V1 最小可运行链落地，不超阶段设计

---

## 12. 一句话收口

`vision/` 目录在当前项目中的唯一正式任务是：

**把现实世界中的资产码，经过采集、识别、标准化、异常拦截与去重后，稳定地转成可提交给后端 `/scan/result` 的正式扫描结果。**

凡超出这句话的职责，默认都不属于 `vision/`。