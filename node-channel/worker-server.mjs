#!/usr/bin/env node
// Ordinary MCP tools for one bounded execution; no Feishu receiver or inbox ownership.
import { spawn } from 'node:child_process';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { CallToolRequestSchema, ListToolsRequestSchema } from '@modelcontextprotocol/sdk/types.js';

const requestPath = process.env.FEISHU_WORKER_REQUEST;
const request = JSON.parse(readFileSync(requestPath, 'utf8'));
const python = request.config.python_command || path.join(request.config.project_dir, '.venv/bin/python');
const schema = (properties, required = Object.keys(properties)) => ({
  type: 'object', properties, required, additionalProperties: false,
});
const string = { type: 'string' };
const positiveId = { type: 'integer', minimum: 1, description: 'Positive numeric ID, never a quoted string.' };
const ids = { type: 'array', items: positiveId, minItems: 1, maxItems: 50 };
const strings = { type: 'array', items: string, minItems: 1, maxItems: 50 };
const gaps = { type: 'array', items: { type: 'string', maxLength: 500 }, maxItems: 20 };
const libraArguments = schema({
  experiment_id: positiveId, app_id: positiveId, metric_group: positiveId,
  base_vid: positiveId, base_version_id: positiveId, bundle_id: positiveId,
  with: { type: 'array', items: { type: 'string', enum: ['versions', 'review', 'relations', 'layer',
    'analysis', 'real_traffic', 'domain_group', 'launch_info'] } },
  with_version_config: { type: 'boolean' }, metric_keys: strings,
  top: { type: 'integer', minimum: 1, maximum: 50 }, workers: { type: 'integer', minimum: 1, maximum: 16 },
  start_date: { type: 'string', description: 'YYYY-MM-DD for d; YYYY-MM-DD HH:MM for h.' },
  end_date: { type: 'string', description: 'Same format as start_date; aligned full-day windows preferred.' },
  period_type: { type: 'string', enum: ['d', 'h'] }, selected_metric_ids: ids, selected_vids: ids,
  version_ids: { ...ids, description: 'Treatment versions ONLY. Exclude base_version_id.' },
  view_type: { type: 'string', enum: ['merge', 'series'] },
  merge_type: { type: 'string', enum: ['avg', 'sum', 'total'] },
  data_region: { type: 'string', enum: ['other', 'eu_ttp', 'tx'] }, combine: { type: 'boolean' },
  mult_cmp_corr: { type: ['boolean', 'integer'], description: 'Boolean for report_data; 0/1 for conclusions.' },
  confidence_threshold: { type: 'number', exclusiveMinimum: 0, exclusiveMaximum: 1 },
  type: strings, global_dimension_with_metric: { type: 'boolean' }, is_bundle_report: { type: 'boolean' },
  global_dimensions: { type: 'array', maxItems: 20,
    items: schema({ global_dim_id: positiveId, global_dim_vals: ids }) },
}, []);
const definitions = [
  { name: 'reply', description: 'Durably save the final answer for this task.',
    inputSchema: schema({ correlation_id: string, text: string,
      business_outcome: { type: 'string', enum: ['completed', 'partial', 'unanswered', 'blocked'] },
      completion_scope: { type: 'string', enum: ['verified', 'advisory'],
        description: 'Default verified. Advisory completes recommendations only, not quantitative verification.' },
      evidence_gaps: gaps },
      ['correlation_id', 'text']) },
  { name: 'checkpoint', description: 'Save replaceable partial findings before more queries. Does not end the task. '
      + 'These findings are delivered if execution later times out. List remaining evidence gaps.',
    inputSchema: schema({ text: { type: 'string', minLength: 1, maxLength: 8000 },
      evidence_gaps: gaps, correlation_id: string }, ['text', 'evidence_gaps']) },
  { name: 'read_image', description: 'Read the authenticated current task image.',
    inputSchema: schema({ correlation_id: string }) },
  { name: 'operations', description: 'Paginated compact summaries of saved operations; use read_artifact for output.',
    inputSchema: schema({ offset: { type: 'integer', minimum: 0 },
      limit: { type: 'integer', minimum: 1, maximum: 50 } }, []) },
  { name: 'read_artifact', description: 'Read saved stdout/stderr for an operation in this task; offsets and limits are bytes.',
    inputSchema: schema({ operation_id: string, artifact: { type: 'string', enum: ['stdout', 'stderr'] },
      offset: { type: 'integer', minimum: 0 }, limit: { type: 'integer', minimum: 1, maximum: 16000 } },
      ['operation_id']) },
  { name: 'read_evidence', description: 'Read compact statistic rows from a saved Libra report_data operation. '
      + 'Preserves metric paths, original units, differences and confidence fields. Prefer to raw JSON paging.',
    inputSchema: schema({ operation_id: string, offset: { type: 'integer', minimum: 0 },
      limit: { type: 'integer', minimum: 1, maximum: 20 } }, ['operation_id']) },
  { name: 'libra_read', description: 'Query Libra with typed arguments and safe read retries. Call action=help once for contracts. '
      + 'Use for experiments, metrics and reports instead of run/Bash. metric_keys and version_ids are JSON arrays. '
      + 'Use the same operation_key to reuse a successful query; changed query semantics need a new key.',
    inputSchema: schema({ operation_key: string,
      action: { type: 'string', enum: ['help', 'experiment_get', 'metric_search', 'report_data',
        'report_bundle', 'important_impact', 'tip_info', 'recycle'] },
      arguments: libraArguments, timeout_seconds: { type: 'integer', minimum: 1, maximum: 180 } },
      ['action']) },
  { name: 'run', description: 'Run a command with durable stdout, exit status and a fixed operation key. '
      + 'May have side effects. Use typed help/search/fetch for reads. Reusing a successful key returns saved results.',
    inputSchema: schema({ operation_key: string, command: string,
      timeout_seconds: { type: 'integer', minimum: 1, maximum: 300 } }, ['operation_key', 'command']) },
  { name: 'create_document', description: 'Create a Feishu document and durably retain its ID and URL. '
      + 'Use the same operation_key for the same document creation across retries.',
    inputSchema: schema({ operation_key: string, title: string, content: string }) },
  { name: 'cli_help', description: 'Read known CLI help with bounded retries; cannot execute arbitrary shell.',
    inputSchema: schema({ operation_key: string, topic: { type: 'string', enum: ['search', 'fetch', 'create'] },
      timeout_seconds: { type: 'integer', minimum: 1, maximum: 60 } }, ['operation_key', 'topic']) },
  { name: 'search_documents', description: 'Search documents as the authenticated user. Read original content to verify snippets.',
    inputSchema: schema({ operation_key: string, query: string,
      page_size: { type: 'integer', minimum: 1, maximum: 20 },
      timeout_seconds: { type: 'integer', minimum: 1, maximum: 60 } }, ['operation_key', 'query']) },
  { name: 'fetch_document', description: 'Read original Markdown using a document ID; safe during write reconciliation.',
    inputSchema: schema({ operation_key: string, document_id: string,
      timeout_seconds: { type: 'integer', minimum: 1, maximum: 60 } }, ['operation_key', 'document_id']) },
];
const integrations = request.config.integrations ?? ['documents', 'libra'];
const documentTools = new Set(['create_document', 'cli_help', 'search_documents', 'fetch_document']);
const enabled = definitions.filter(d => (!documentTools.has(d.name) || integrations.includes('documents'))
  && (!['libra_read', 'read_evidence'].includes(d.name) || integrations.includes('libra')));
const server = new Server({ name: 'feishu', version: '0.3.1' }, { capabilities: { tools: {} } });
server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: enabled }));
server.setRequestHandler(CallToolRequestSchema, async ({ params }) => {
  if (!enabled.some(d => d.name === params.name)) throw new Error('Unknown or disabled tool');
  const result = await new Promise((resolve, reject) => {
    const child = spawn(python, ['-m', 'feishu_llm_bot.task_tools'], {
      env: { ...process.env, PYTHONPATH: path.join(request.config.project_dir, 'src') },
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    const chunks = [];
    let size = 0;
    child.stdout.on('data', data => {
      size += data.length;
      if (size > 8 * 1024 * 1024) { child.kill(); reject(new Error('Tool output too large')); }
      else chunks.push(data);
    });
    child.stderr.resume();
    child.on('error', reject);
    child.on('close', () => {
      try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8'))); }
      catch { reject(new Error('Tool exited without a valid result; check saved operations')); }
    });
    child.stdin.on('error', reject);
    child.stdin.end(JSON.stringify({ name: params.name, arguments: params.arguments ?? {} }));
  });
  if (result.error) return { isError: true, content: [{ type: 'text', text: result.error }] };
  if (result.result?.image) return { content: [{ type: 'image', data: result.result.image,
    mimeType: result.result.mime_type }] };
  return { isError: ['failed', 'unknown'].includes(result.result?.state),
    content: [{ type: 'text', text: JSON.stringify(result.result) }] };
});
await server.connect(new StdioServerTransport());
