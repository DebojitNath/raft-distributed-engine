"""Asynchronous RPC Layer and Fault Injection Network Abstraction.

Reference: Raft Paper Section 5.1 & ARCHITECTURE.md Section 2 (JSON over TCP).
Uses pure asyncio (StreamReader/StreamWriter) with newline-delimited framing
and configurable packet drop filters for chaos partition testing.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict, Iterable, Optional, Set, Tuple

from raft.messages import (
    AppendEntriesArgs,
    AppendEntriesReply,
    RequestVoteArgs,
    RequestVoteReply,
    RPCMessage,
    deserialize_envelope,
    serialize_envelope,
)

logger = logging.getLogger(__name__)


class RPCManager:
    """Manages asynchronous TCP communication and network fault injection.

    Attributes:
        node_id: Identifier of the local Raft node.
        host: Host address to bind the TCP server (default: 127.0.0.1).
        port: Port to bind the TCP server (0 = ephemeral port for testing).
        peer_addresses: Dict mapping peer node IDs to (host, port) tuples.
        ignore_list: Set of peer node IDs whose messages should be dropped
            (simulates network partitions / severed cables).
    """

    def __init__(
        self,
        node_id: str,
        host: str = "127.0.0.1",
        port: int = 0,
        peer_addresses: Optional[Dict[str, Tuple[str, int]]] = None,
    ) -> None:
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peer_addresses: Dict[str, Tuple[str, int]] = dict(peer_addresses or {})
        self.ignore_list: Set[str] = set()

        # Server instance
        self._server: Optional[asyncio.Server] = None
        self._is_running = False

        # RPC Handlers (to be registered by RaftNode)
        self.request_vote_handler: Optional[
            Callable[[RequestVoteArgs], Awaitable[RequestVoteReply]]
        ] = None
        self.append_entries_handler: Optional[
            Callable[[AppendEntriesArgs], Awaitable[AppendEntriesReply]]
        ] = None

    @property
    def is_running(self) -> bool:
        """Return True if the RPC server is actively listening."""
        return self._is_running

    # -------------------------------------------------------------------------
    # Fault Injection Controls (God Mode / Chaos Engineering)
    # -------------------------------------------------------------------------

    def set_ignore_list(self, peer_ids: Iterable[str]) -> None:
        """Replace the ignore list with a new set of peer IDs."""
        self.ignore_list = set(peer_ids)
        logger.info("Node %s updated ignore_list to %s", self.node_id, self.ignore_list)

    def add_ignored(self, peer_id: str) -> None:
        """Add a peer ID to the ignore list (sever connection)."""
        self.ignore_list.add(peer_id)
        logger.info("Node %s now ignoring peer %s", self.node_id, peer_id)

    def remove_ignored(self, peer_id: str) -> None:
        """Remove a peer ID from the ignore list (heal connection)."""
        self.ignore_list.discard(peer_id)
        logger.info("Node %s restored connection to peer %s", self.node_id, peer_id)

    def clear_ignored(self) -> None:
        """Clear all ignored peers (heal all partitions)."""
        self.ignore_list.clear()
        logger.info("Node %s cleared ignore_list (all links healed)", self.node_id)

    def is_ignored(self, peer_id: str) -> bool:
        """Return True if communication with peer_id is currently blocked."""
        return peer_id in self.ignore_list

    # -------------------------------------------------------------------------
    # Server Lifecycle & Connection Handling
    # -------------------------------------------------------------------------

    async def start(self) -> None:
        """Start the asyncio TCP server."""
        if self._is_running:
            return

        self._server = await asyncio.start_server(
            self._handle_client,
            self.host,
            self.port,
        )
        # If port was 0, capture the actual assigned ephemeral port
        sockets = self._server.sockets
        if sockets:
            self.port = sockets[0].getsockname()[1]

        self._is_running = True
        logger.info("RPCManager for %s listening on %s:%d", self.node_id, self.host, self.port)

    async def stop(self) -> None:
        """Stop the asyncio TCP server and close all active sockets."""
        if not self._is_running or self._server is None:
            return

        self._is_running = False
        self._server.close()
        await self._server.wait_closed()
        self._server = None
        logger.info("RPCManager for %s stopped", self.node_id)

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handle incoming TCP connections and route decoded RPCs."""
        try:
            while self._is_running:
                raw_line = await reader.readline()
                if not raw_line:
                    break

                try:
                    _, sender_id, message = deserialize_envelope(raw_line.decode())
                except Exception as e:
                    logger.warning("Node %s failed to parse envelope: %s", self.node_id, e)
                    break

                # Fault Injection Check (Incoming partition)
                if self.is_ignored(sender_id):
                    logger.debug(
                        "Node %s dropping incoming message from ignored peer %s",
                        self.node_id,
                        sender_id,
                    )
                    # Silently close connection / ignore
                    break

                # Dispatch message to registered handler
                reply: Optional[RPCMessage] = None
                if isinstance(message, RequestVoteArgs) and self.request_vote_handler:
                    reply = await self.request_vote_handler(message)
                elif isinstance(message, AppendEntriesArgs) and self.append_entries_handler:
                    reply = await self.append_entries_handler(message)

                if reply is not None:
                    # Fault Injection Check (Outgoing partition reply)
                    if self.is_ignored(sender_id):
                        logger.debug(
                            "Node %s dropping outgoing reply to ignored peer %s",
                            self.node_id,
                            sender_id,
                        )
                        break

                    response_data = serialize_envelope(self.node_id, reply)
                    writer.write(response_data.encode())
                    await writer.drain()

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.debug("Node %s client connection handler error: %s", self.node_id, e)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    # -------------------------------------------------------------------------
    # Client RPC Dispatch
    # -------------------------------------------------------------------------

    async def call_rpc(
        self,
        target_node_id: str,
        message: RPCMessage,
        timeout: float = 0.5,
    ) -> Optional[RPCMessage]:
        """Send a typed RPC to target peer and await reply.

        Returns None if target is unreachable, timed out, or blocked by ignore_list.
        """
        # Fault Injection Check (Outgoing partition)
        if self.is_ignored(target_node_id):
            logger.debug(
                "Node %s dropping outgoing RPC to ignored peer %s",
                self.node_id,
                target_node_id,
            )
            return None

        if target_node_id not in self.peer_addresses:
            logger.warning("Node %s has no address for peer %s", self.node_id, target_node_id)
            return None

        host, port = self.peer_addresses[target_node_id]

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=timeout,
            )
        except (asyncio.TimeoutError, ConnectionRefusedError, OSError) as e:
            logger.debug(
                "Node %s connection to %s (%s:%d) failed: %s",
                self.node_id,
                target_node_id,
                host,
                port,
                e,
            )
            return None

        try:
            # Send serialized newline-delimited envelope
            envelope = serialize_envelope(self.node_id, message)
            writer.write(envelope.encode())
            await writer.drain()

            # Read response
            raw_reply = await asyncio.wait_for(reader.readline(), timeout=timeout)
            if not raw_reply:
                return None

            _, _, reply = deserialize_envelope(raw_reply.decode())
            return reply

        except (asyncio.TimeoutError, ConnectionResetError, OSError) as e:
            logger.debug(
                "Node %s RPC to %s timed out or failed: %s",
                self.node_id,
                target_node_id,
                e,
            )
            return None
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def send_request_vote(
        self,
        target_node_id: str,
        args: RequestVoteArgs,
        timeout: float = 0.5,
    ) -> Optional[RequestVoteReply]:
        """Convenience method to send RequestVote RPC."""
        reply = await self.call_rpc(target_node_id, args, timeout=timeout)
        if isinstance(reply, RequestVoteReply):
            return reply
        return None

    async def send_append_entries(
        self,
        target_node_id: str,
        args: AppendEntriesArgs,
        timeout: float = 0.5,
    ) -> Optional[AppendEntriesReply]:
        """Convenience method to send AppendEntries RPC."""
        reply = await self.call_rpc(target_node_id, args, timeout=timeout)
        if isinstance(reply, AppendEntriesReply):
            return reply
        return None
