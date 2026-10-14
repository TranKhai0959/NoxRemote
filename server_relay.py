import os
import sys
import asyncio
import json
import logging
import http
import websockets
from typing import Dict, Optional

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [RelayServer] %(message)s")
logger = logging.getLogger("NoxRelay")

# Connected peers: device_id -> WebSocketServerProtocol
peers: Dict[str, any] = {}
# Active sessions: ws -> partner_ws
rooms: Dict[any, any] = {}

async def process_request(connection, request):
    """Handles HTTP GET requests (Health checks & UptimeRobot pings to keep 24/7 online)."""
    # If client is requesting a normal web page or health check (not WebSocket handshake)
    upgrade = request.headers.get("Upgrade", "").lower()
    if upgrade != "websocket" and request.path in ("/", "/health", "/healthz", "/ping"):
        html_content = (
            "<!DOCTYPE html>"
            "<html>"
            "<head><title>NoxRemote Relay Server</title></head>"
            "<body style='font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif; background: #0f172a; color: #f8fafc; text-align: center; padding-top: 80px;'>"
            "<div style='display:inline-block; background: #1e293b; padding: 30px 50px; border-radius: 16px; border: 1px solid #38bdf8; box-shadow: 0 10px 25px rgba(0,0,0,0.5);'>"
            "<h1 style='color: #38bdf8; margin-bottom: 10px;'>&#9989; NoxRemote Relay Server is ONLINE</h1>"
            "<p style='color: #94a3b8; font-size: 15px;'>Máy chủ kết nối xuyên mạng 24/7 đang hoạt động bình thường.</p>"
            f"<p style='color: #34d399; font-size: 13px;'>Số máy chủ đang trực tuyến: <b>{len(peers)}</b></p>"
            "</div>"
            "</body>"
            "</html>"
        )
        return connection.respond(
            http.HTTPStatus.OK,
            [
                ("Content-Type", "text/html; charset=utf-8"),
                ("Access-Control-Allow-Origin", "*"),
            ],
            html_content.encode("utf-8")
        )
    return None

async def handle_client(websocket):
    current_device_id = None
    try:
        async for message in websocket:
            # If in an active session, forward raw binary or text data directly to partner!
            if websocket in rooms:
                partner = rooms[websocket]
                try:
                    await partner.send(message)
                except Exception:
                    # Partner disconnected
                    if websocket in rooms:
                        del rooms[websocket]
                    if partner in rooms:
                        del rooms[partner]
                continue

            # Otherwise, parse signaling command (JSON)
            if isinstance(message, str):
                try:
                    data = json.loads(message)
                except Exception:
                    continue

                action = data.get("action")
                
                # 1. Register Host device ID
                if action == "register":
                    raw_id = data.get("device_id", "").replace(" ", "").strip()
                    if raw_id:
                        current_device_id = raw_id
                        peers[raw_id] = websocket
                        logger.info(f"Host registered: {raw_id} (Total hosts: {len(peers)})")
                        await websocket.send(json.dumps({
                            "type": "register_ack",
                            "status": "ok",
                            "device_id": raw_id
                        }))

                # 2. Client initiates connection to target Host
                elif action == "connect":
                    target_id = data.get("target_id", "").replace(" ", "").strip()
                    client_id = data.get("client_id", "").replace(" ", "").strip()
                    logger.info(f"Client {client_id} attempting to connect to Host {target_id}")

                    if target_id not in peers:
                        await websocket.send(json.dumps({
                            "type": "connect_failed",
                            "reason": f"Không tìm thấy Host với ID {target_id}. Hãy kiểm tra xem Host đã bật NoxRemote chưa."
                        }))
                        continue

                    target_ws = peers[target_id]
                    # Establish bidirectional room bridge
                    rooms[websocket] = target_ws
                    rooms[target_ws] = websocket

                    # Notify both parties that bridge is ready for handshake
                    bridge_msg = json.dumps({"type": "bridge_ready"})
                    await websocket.send(bridge_msg)
                    await target_ws.send(bridge_msg)
                    logger.info(f"Bridge established between Client {client_id} and Host {target_id}")

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        logger.error(f"Error handling peer: {e}")
    finally:
        # Cleanup
        if current_device_id and current_device_id in peers and peers[current_device_id] == websocket:
            del peers[current_device_id]
            logger.info(f"Host disconnected: {current_device_id}")

        if websocket in rooms:
            partner = rooms[websocket]
            del rooms[websocket]
            if partner in rooms:
                del rooms[partner]
            try:
                await partner.send(json.dumps({"type": "partner_disconnected"}))
            except Exception:
                pass

async def main(host: str = "0.0.0.0", port: Optional[int] = None):
    if port is None:
        port = int(os.environ.get("PORT", sys.argv[1] if len(sys.argv) > 1 else 8765))

    logger.info(f"Starting NoxRemote Signaling & Relay Server on {host}:{port}...")
    async with websockets.serve(
        handle_client,
        host,
        port,
        process_request=process_request,
        max_size=100 * 1024 * 1024
    ):
        logger.info(f"Relay Server is ready to route cross-network connections on port {port} (HTTP & WSS supported).")
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server stopped.")
