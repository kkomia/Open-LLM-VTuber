# LLMVTuber 频道插件

把Open-LLM-VTuber桌面端（Live2D桌宠）跟QwenPaw Agent后端连起来的WebSocket通道。

## 架构

```
┌──────────────────┐    WebSocket     ┌─────────────────────────┐
│ LLMVTuber 桌面端    │ ←────────────→ │  QwenPaw llmvtuber频道   │
│ (Live2D + 前端)   │  ws://:8765     │  custom_channels/       │
└──────────────────┘                  └──────────┬──────────────┘
                                                 │ ACP
                                          ┌──────▼──────┐
                                          │  d老师 Agent  │
                                          │ (MCP工具等)    │
                                          └─────────────┘
```

## 文件结构

| 文件 | 说明 |
|:---|:---|
| `custom_channels/llmvtuber_channel.py` | 频道实现，继承BaseChannel |
| `doc/ws_protocol.md` | WebSocket协议文档 |

## 配置

在agent.json的channels里配：

```json
"llmvtuber": {
  "enabled": true,
  "host": "0.0.0.0",
  "port": 8765
}
```

## WebSocket消息协议

详见 `doc/ws_protocol.md`

### 前端→后端
- `text-input` — 文字消息
- `heartbeat` — 心跳
- `interrupt-signal` — 打断回复

### 后端→前端
- `set-model-and-conf` — 初始化
- `full-text` — AI回复
- `heartbeat-ack` — 心跳回复
- `error` — 错误信息

## 开发

```bash
# 装依赖
pip install websockets

# 测试
python3 -m pytest tests/
```
