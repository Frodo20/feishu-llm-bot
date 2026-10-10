#!/usr/bin/env node
// Isolated runtime database + local fake CLI; no model, Feishu receiver or sender.
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';

const project = fileURLToPath(new URL('..', import.meta.url));
const python = path.join(project, '.venv/bin/python');
const root = await mkdtemp(path.join(os.tmpdir(), 'feishu-worker-mcp-test-'));
const requestPath = path.join(root, 'request.json');
const setup = `
import json,shlex,sys,time
from pathlib import Path
from feishu_llm_bot.runtime_store import RuntimeStore
from feishu_llm_bot.runtime_common import private_json
root,project=map(Path,sys.argv[1:])
cli=root/'cli'
code='import json;print(json.dumps({"ok":True,"data":{"results":[]}}))'
cli.write_text('#!/bin/sh\\nexec '+shlex.quote(sys.executable)+' -c '+shlex.quote(code)+'\\n')
cli.chmod(0o700)
store=RuntimeStore(root/'bot.sqlite3')
store.accept_event('probe','local-only','isolated MCP verification')
a=store.claim(time.time())
private_json(root/'request.json',{**a,'config':{'project_dir':str(project),'database_path':str(store.path),'bytedcli_command':str(cli)}})
store.close()
`;
const client = new Client({ name: 'worker-reliability-test', version: '1.0.0' });
try {
  execFileSync(python, ['-c', setup, root, project], {
    env: { ...process.env, PYTHONPATH: path.join(project, 'src') },
  });
  const transport = new StdioClientTransport({
    command: process.execPath, args: [path.join(project, 'node-channel/worker-server.mjs')],
    env: { ...process.env, FEISHU_WORKER_REQUEST: requestPath }, stderr: 'pipe',
  });
  await client.connect(transport);
  const definitions = await client.listTools();
  assert.equal(definitions.tools.length, 12);
  const libraSchema = definitions.tools.find(tool => tool.name === 'libra_read').inputSchema;
  assert.equal(libraSchema.properties.arguments.properties.experiment_id.type, 'integer');
  assert.match(libraSchema.properties.arguments.properties.version_ids.description, /Exclude/);
  const checkpoint = await client.callTool({ name: 'checkpoint', arguments: {
    text: 'Initial evidence recorded', evidence_gaps: ['More evidence needed'],
  } });
  assert.equal(checkpoint.isError, false);
  for (const name of ['cli_help', 'search_documents', 'fetch_document', 'libra_read', 'read_artifact', 'read_evidence']) {
    assert.ok(definitions.tools.some(tool => tool.name === name));
  }
  const searched = await client.callTool({ name: 'search_documents', arguments: {
    operation_key: 'search', query: 'literal ; $(text)',
  } });
  assert.equal(searched.isError, false);
  assert.equal(JSON.parse(searched.content[0].text).state, 'succeeded');
  const saved = await client.callTool({ name: 'operations', arguments: {} });
  const op = JSON.parse(saved.content[0].text).operations[0];
  assert.equal(op.kind, 'search_documents');
  const artifact = await client.callTool({ name: 'read_artifact', arguments: { operation_id: op.operation_id } });
  assert.match(JSON.parse(artifact.content[0].text).text, /results/);
  const libraHelp = await client.callTool({ name: 'libra_read', arguments: { action: 'help' } });
  assert.ok(JSON.parse(libraHelp.content[0].text).actions.metric_search);
  const rejected = await client.callTool({ name: 'search_documents', arguments: {
    operation_key: 'search', query: 'different semantic input',
  } });
  assert.equal(rejected.isError, true);
  const help = await client.callTool({ name: 'cli_help', arguments: {
    operation_key: 'help', topic: 'search', timeout_seconds: 1,
  } });
  assert.equal(help.isError, true);
  assert.equal(JSON.parse(help.content[0].text).state, 'failed');
  console.log('worker MCP: typed tools, durable results, semantic fencing and failure status passed');
} finally {
  await client.close().catch(() => {});
  await rm(root, { recursive: true, force: true });
}
