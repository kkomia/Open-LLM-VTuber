"""
LLMVTuber Channel - WebSocket通道，对接Open-LLM-VTuber前端
前端Live2D桌面端通过WebSocket连到这个频道，跟QwenPaw Agent对话
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any, Dict, Optional, Union

try:
    import websockets
except ImportError:
    websockets = None  # type: ignore

from agentscope_runtime.engine.schemas.agent_schemas import (
    TextContent,
    ContentType,
)

from qwenpaw.app.channels.base import BaseChannel, OnReplySent, ProcessHandler, OutgoingContentPart

logger = logging.getLogger(__name__)

# WebSocket 消息类型（跟LLMVTuber前端约定好的）
# =============  前端 -> Agent  =============
MSG_TEXT_INPUT = "text-input"       # {type, text, images?}
MSG_HEARTBEAT = "heartbeat"          # 心跳
MSG_INTERRUPT = "interrupt-signal"   # 打断

# =============  Agent -> 前端  =============
MSG_FULL_TEXT = "full-text"          # {type, text, display_text?}
MSG_HEARTBEAT_ACK = "heartbeat-ack"
MSG_ERROR = "error"                  # {type, message}


class LLMVTuberChannel(BaseChannel):
    """WebSocket频道，让Open-LLM-VTuber桌面端连过来跟Agent对话"""

    channel = "llmvtuber"
    uses_manager_queue = True

    def __init__(
        self,
        process: ProcessHandler,
        enabled: bool = True,
        host: str = "0.0.0.0",
        port: int = 8765,
        max_connections: int = 10,
        on_reply_sent: OnReplySent = None,
        show_tool_details: bool = True,
        filter_tool_messages: bool = False,
        filter_thinking: bool = False,
    ):
        super().__init__(
            process,
            on_reply_sent=on_reply_sent,
            show_tool_details=show_tool_details,
            filter_tool_messages=filter_tool_messages,
            filter_thinking=filter_thinking,
        )
        self.enabled = enabled
        self.host = host
        self.port = port
        self.max_connections = max_connections

        # WebSocket 服务端
        self._server: Optional[websockets.WebSocketServer] = None
        self._connections: Dict[str, Any] = {}  # uid -> websocket
        self._user_id_map: Dict[str, str] = {}  # uid -> user_id

    # ==================== 生命周期 ====================

    @classmethod
    def from_config(
        cls,
        process: ProcessHandler,
        config: Union[dict, Any],
        on_reply_sent: OnReplySent = None,
        show_tool_details: bool = True,
        filter_tool_messages: bool = False,
        filter_thinking: bool = False,
    ) -> "LLMVTuberChannel":
        if isinstance(config, dict):
            return cls(
                process=process,
                enabled=bool(config.get("enabled", True)),
                host=str(config.get("host", "0.0.0.0")),
                port=int(config.get("port", 8765)),
                on_reply_sent=on_reply_sent,
                show_tool_details=show_tool_details,
                filter_tool_messages=filter_tool_messages,
                filter_thinking=filter_thinking,
            )
        return cls(process=process)

    async def start(self) -> None:
        """启动WebSocket服务端，等前端连过来"""
        if not self.enabled:
            logger.info("LLMVTuber channel disabled, skipping start")
            return

        if websockets is None:
            logger.error(
                "websockets库没装，装一下: pip install websockets"
            )
            return

        try:
            self._server = await websockets.serve(
                self._handle_ws_connection,
                self.host,
                self.port,
                max_size=2**20,  # 1MB
                ping_interval=30,
                ping_timeout=10,
                max_connections=self.max_connections,
            )
            logger.info(
                f"LLMVTuber WebSocket 服务已启动: ws://{self.host}:{self.port}"
            )
        except OSError as e:
            logger.error(f"LLMVTuber WebSocket 端口被占用 ({self.host}:{self.port}): {e}")
        except Exception as e:
            logger.error(f"LLMVTuber WebSocket 启动失败: {e}")

    async def stop(self) -> None:
        """关掉WebSocket服务端"""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            logger.info("LLMVTuber WebSocket 服务已关闭")

        # 断开所有连接
        for uid, ws in list(self._connections.items()):
            try:
                await ws.close()
            except Exception:
                pass
        self._connections.clear()
        self._user_id_map.clear()

    # ==================== WebSocket 处理 ====================

    async def _handle_ws_connection(self, ws) -> None:
        """有新前端连过来了"""
        uid = str(uuid.uuid4())
        self._connections[uid] = ws
        logger.info(f"LLMVTuber 前端已连接: {uid}")

        try:
            # 初始化：发个set-model-and-conf让前端知道连上了
            await self._send_json(ws, {
                "type": "set-model-and-conf",
                "model": "qwenpaw-agent",
                "config": {"name": "d老师"},
                "language": "zh-CN",
            })

            # 开始收消息
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    await self._on_message(uid, msg)
                except json.JSONDecodeError:
                    logger.warning(f"收到非法JSON: {raw[:100]}")

        except Exception as e:
            logger.warning(f"连接异常断开 [{uid}]: {e}")
        finally:
            self._connections.pop(uid, None)
            self._user_id_map.pop(uid, None)

    async def _on_message(self, uid: str, msg: dict) -> None:
        """处理前端发来的消息"""
        msg_type = msg.get("type", "")

        if msg_type == MSG_HEARTBEAT:
            await self._send_to(uid, {"type": "heartbeat-ack"})
            return

        if msg_type == MSG_INTERRUPT:
            logger.info(f"收到打断信号: {uid}")
            return

        if msg_type == MSG_TEXT_INPUT:
            text = msg.get("text", "")
            if text:
                await self._process_text_input(uid, text)
            return

        logger.debug(f"未处理的消息类型: {msg_type}")

    async def _process_text_input(self, uid: str, text: str) -> None:
        """把文字消息转成AgentRequest，交给QwenPaw处理"""
        user_id = self._user_id_map.get(uid, f"llmvtuber_{uid[:8]}")
        session_id = f"llmvtuber_{uid[:8]}"

        # 先用thinking消息让前端知道在想了
        await self._send_to(uid, {"type": "full-text", "text": "🤔 ...", "display_text": {"role": "thinking"}})

        # 走QwenPaw的process handler
        from agentscope_runtime.engine.schemas.agent_schemas import (
            AgentRequest,
            Message,
            TextContent,
            ContentType,
        )

        req = AgentRequest(
            session_id=session_id,
            user_id=user_id,
            input=[Message(role="user", content=[TextContent(text=text)])],
        )

        try:
            async for event in self._process(req):
                if event.type == "message" and event.contents:
                    for content in event.contents:
                        if content.type == ContentType.TEXT and content.text:
                            await self._send_to(uid, {"type": "full-text", "text": content.text})
        except Exception as e:
            logger.error(f"Agent处理消息出错: {e}")
            await self._send_to(uid, {"type": "error", "message": f"处理出错: {e}"})

    # ==================== QwenPaw 接口方法 ====================

    async def send(
        self,
        user_id: str,
        session_id: str,
        parts: list[OutgoingContentPart],
        to_handle: str | None = None,
        reply_to_message_id: str | None = None,
    ) -> None:
        """QwenPaw调用这个方法来发消息给用户"""
        text_parts = [p for p in parts if p.type == ContentType.TEXT]
        if text_parts:
            full_text = "\n".join(p.text for p in text_parts)
            await self._send_to_by_session(
                session_id,
                {"type": "full-text", "text": full_text},
            )

    async def send_media(
        self,
        user_id: str,
        session_id: str,
        parts: list[OutgoingContentPart],
        to_handle: str | None = None,
        reply_to_message_id: str | None = None,
    ) -> None:
        """发图片/文件给前端（暂时不支持，后续搞）"""
        logger.debug(f"send_media called but not implemented: {session_id}")

    # ==================== 工具方法 ====================

    async def _send_json(self, ws, data: dict) -> None:
        """给某个WebSocket连接发JSON消息"""
        try:
            await ws.send(json.dumps(data, ensure_ascii=False))
        except Exception as e:
            logger.warning(f"发消息失败: {e}")

    async def _send_to(self, uid: str, data: dict) -> None:
        """根据连接UID发消息"""
        ws = self._connections.get(uid)
        if ws:
            await self._send_json(ws, data)

    async def _send_to_by_session(self, session_id: str, data: dict) -> None:
        """根据会话ID发消息"""
        uid = session_id.replace("llmvtuber_", "")
        await self._send_to(uid, data)
