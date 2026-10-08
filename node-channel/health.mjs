import { renameSync, writeFileSync } from "node:fs";

// Local supervisor metadata only: never include inbox credentials or message text.
export function startHealthReporter(file, instance, snapshot) {
  if (!file) return;
  const temporary = `${file}.${process.pid}.tmp`;
  const report = () => {
    try {
      writeFileSync(temporary, JSON.stringify({
        instance,
        timestamp: Date.now() / 1000,
        mcp_pid: process.pid,
        ...snapshot(),
      }), { mode: 0o600 });
      renameSync(temporary, file);
    } catch {
      // A missing/stale heartbeat makes the supervisor restart this process tree.
      process.stderr.write("feishu bridge: cannot publish supervisor health\n");
    }
  };
  report();
  setInterval(report, 5000).unref();
}
