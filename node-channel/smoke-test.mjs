#!/usr/bin/env node

import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";

const serverPath = fileURLToPath(new URL("./server.mjs", import.meta.url));
const fakeBridgePath = fileURLToPath(new URL("../tests/fake_bridge.py", import.meta.url));
const pythonPath = process.env.FEISHU_BRIDGE_TEST_PYTHON
  ?? fileURLToPath(new URL("../.venv/bin/python", import.meta.url));
const envFile = process.env.FEISHU_BRIDGE_TEST_ENV_FILE
  ?? fileURLToPath(new URL("../.env.test", import.meta.url));
const correlationId = "fs_0123456789abcdef0123456789abcdef";
const inboxToken = "test-child-token-never-log";
const imageData = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=";
const temporaryDirectory = await mkdtemp(path.join(os.tmpdir(), "feishu-inbox-test-"));
const socketPath = path.join(temporaryDirectory, "inbox.sock");
const inboxConnections = [];
let resolveInbox;
const inboxReceived = new Promise((resolve) => {
  resolveInbox = resolve;
});

const inboxServer = net.createServer({ allowHalfOpen: true }, (socket) => {
  let buffer = "";
  socket.setEncoding("utf8");
  socket.on("data", (chunk) => {
    buffer += chunk;
  });
  socket.on("end", () => {
    inboxConnections.push(buffer);
    socket.end();
    resolveInbox();
  });
});
await new Promise((resolve, reject) => {
  inboxServer.once("error", reject);
  inboxServer.listen(socketPath, resolve);
});

const client = new Client({ name: "feishu-session-bridge-smoke", version: "1.0.0" });
const transport = new StdioClientTransport({
  command: process.execPath,
  args: [serverPath],
  env: {
    ...process.env,
    CLAUDE_CODE_MESSAGING_SOCKET: socketPath,
    CLAUDE_CODE_MESSAGING_TOKEN: inboxToken,
    FEISHU_BRIDGE_ENV_FILE: envFile,
    FEISHU_BRIDGE_EXECUTABLE: pythonPath,
    FEISHU_BRIDGE_ARGS: JSON.stringify([fakeBridgePath]),
    FEISHU_FAKE_MESSAGE_TYPE: "image",
  },
  stderr: "pipe",
});

let stderr = "";
transport.stderr?.setEncoding("utf8");
transport.stderr?.on("data", (chunk) => {
  stderr += chunk;
});

try {
  await client.connect(transport);
  const tools = await client.listTools();
  assert.deepEqual(tools.tools.map((tool) => tool.name), ["reply", "read_image"]);
  const readImageTool = tools.tools.find((tool) => tool.name === "read_image");
  assert.deepEqual(readImageTool.inputSchema, {
    type: "object",
    properties: {
      correlation_id: {
        type: "string",
        description: "Opaque correlation_id stated by the Feishu bridge image message.",
      },
    },
    required: ["correlation_id"],
    additionalProperties: false,
  });
  assert.deepEqual(readImageTool.annotations, {
    readOnlyHint: true,
    destructiveHint: false,
    idempotentHint: true,
    openWorldHint: false,
  });
  assert.deepEqual(client.getServerCapabilities(), { tools: {} });

  await Promise.race([
    inboxReceived,
    new Promise((_, reject) => {
      setTimeout(() => reject(new Error("inbox message timed out")), 3000);
    }),
  ]);
  assert.equal(inboxConnections.length, 1);
  const frames = inboxConnections[0]
    .trimEnd()
    .split("\n")
    .map((line) => JSON.parse(line));
  assert.deepEqual(frames[0], { type: "auth", token: inboxToken });
  assert.equal(frames[1].type, "user");
  assert.equal(frames[1].message.role, "user");
  assert.match(frames[1].message.content, /\[Feishu bridge image\]/);
  assert.match(frames[1].message.content, new RegExp(correlationId));
  assert.match(frames[1].message.content, /mcp__feishu__read_image/);
  assert.doesNotMatch(
    frames[1].message.content,
    /hello from Feishu|chat-1|message-1|test-child-token-never-log/,
  );

  const invalid = await client.callTool({
    name: "reply",
    arguments: { correlation_id: "invented", text: "no" },
  });
  assert.equal(invalid.isError, true);

  const image = await client.callTool({
    name: "read_image",
    arguments: { correlation_id: correlationId },
  });
  assert.equal(image.isError, undefined);
  assert.deepEqual(image.content, [{
    type: "image",
    data: imageData,
    mimeType: "image/png",
  }]);

  const imageAgain = await client.callTool({
    name: "read_image",
    arguments: { correlation_id: correlationId },
  });
  assert.equal(imageAgain.isError, undefined);
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.equal(inboxConnections.length, 1);

  const invalidImage = await client.callTool({
    name: "read_image",
    arguments: { correlation_id: correlationId, extra: true },
  });
  assert.equal(invalidImage.isError, true);

  const staleCorrelation = "fs_ffffffffffffffffffffffffffffffff";
  const staleImage = await client.callTool({
    name: "read_image",
    arguments: { correlation_id: staleCorrelation },
  });
  assert.equal(staleImage.isError, true);
  const staleReply = await client.callTool({
    name: "reply",
    arguments: { correlation_id: staleCorrelation, text: "wrong turn" },
  });
  assert.equal(staleReply.isError, true);

  const reply = await client.callTool({
    name: "reply",
    arguments: { correlation_id: correlationId, text: "answer" },
  });
  assert.equal(reply.isError, undefined);
  assert.equal(reply.content[0].text, "sent 1 part(s)");
  assert.doesNotMatch(stderr, /test-child-token-never-log|hello from Feishu/);
} finally {
  await client.close();
  await new Promise((resolve) => inboxServer.close(resolve));
  await rm(temporaryDirectory, { recursive: true, force: true });
}
