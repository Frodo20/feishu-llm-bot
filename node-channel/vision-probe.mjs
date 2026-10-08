#!/usr/bin/env node
// Optional live Claude vision integration fixture. No Feishu credentials/API.
// Supplies only an image content block, the same format as feishu/read_image.
import { readFileSync, writeFileSync } from "node:fs";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { CallToolRequestSchema, ListToolsRequestSchema } from "@modelcontextprotocol/sdk/types.js";

const bytes = readFileSync(process.argv[2]);
const evidence = process.argv[3];
const server = new Server({ name: "vision_probe", version: "1.0.0" }, { capabilities: { tools: {} } });
server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: [{
  name: "read_image", description: "Read the test image for visual analysis.",
  inputSchema: { type: "object", properties: {}, additionalProperties: false },
  annotations: { readOnlyHint: true },
}] }));
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  if (request.params.name !== "read_image") throw new Error("Unknown tool");
  writeFileSync(evidence, JSON.stringify({ called: true, type: "image", bytes: bytes.length }),
    { mode: 0o600 });
  return { content: [{ type: "image", mimeType: "image/png", data: bytes.toString("base64") }] };
});
await server.connect(new StdioServerTransport());
