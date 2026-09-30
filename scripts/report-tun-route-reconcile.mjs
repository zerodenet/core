import { appendFileSync, readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

export function classifyCoverage(report, testExitCode) {
  if (testExitCode !== 0) throw new Error(`route-switch test command failed (${testExitCode})`);
  if (report?.scenario !== 'windows-route-switch'
      || !['passed', 'skipped'].includes(report.status)
      || typeof report.reason !== 'string' || !report.reason.trim()
      || report.executed !== (report.status === 'passed')) {
    throw new Error('missing or invalid Windows route-switch execution evidence');
  }
  return {
    status: report.status,
    level: report.status === 'passed' ? 'notice' : 'warning',
    message: report.status === 'passed'
      ? `Windows route-switch scenario executed and passed: ${report.reason}`
      : `Windows route-switch scenario SKIPPED; no network-switch coverage: ${report.reason}`,
  };
}

function annotate(level, message) {
  const escaped = message.replaceAll('%', '%25').replaceAll('\r', '%0D').replaceAll('\n', '%0A');
  console.log(`::${level} title=Windows route-switch coverage::${escaped}`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const report = JSON.parse(readFileSync(process.argv[2], 'utf8'));
    const result = classifyCoverage(report, Number(process.argv[3]));
    annotate(result.level, result.message);
    if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, `status=${result.status}\n`);
    if (process.env.GITHUB_STEP_SUMMARY) appendFileSync(process.env.GITHUB_STEP_SUMMARY, `${result.message}\n`);
  } catch (error) {
    annotate('error', error.message);
    if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, 'status=failed\n');
    process.exitCode = 1;
  }
}
