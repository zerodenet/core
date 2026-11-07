// Collect this exact workflow's existing inputs and original job logs only.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const sha = bytes => crypto.createHash('sha256').update(bytes).digest('hex');

module.exports = async ({ github, context, core }) => {
  const output = '.round2-evidence/collected';
  fs.mkdirSync(path.join(output, 'logs'), { recursive: true });
  const candidate = context.payload.pull_request?.head.sha ?? context.sha;
  const jobs = await github.paginate(github.rest.actions.listJobsForWorkflowRun, {
    ...context.repo, run_id: context.runId, filter: 'latest', per_page: 100,
  });
  const records = [];
  let unavailable = false;
  for (const job of jobs) {
    const record = { id: job.id, name: job.name, status: job.status,
      conclusion: job.conclusion, started_at: job.started_at,
      completed_at: job.completed_at, steps: job.steps };
    if (job.conclusion === 'skipped') {
      record.raw_log_unavailable = { reason: 'job was not executed' };
    } else if (job.status === 'completed' && job.started_at) {
      try {
        const response = await github.rest.actions.downloadJobLogsForWorkflowRun({
          ...context.repo, job_id: job.id,
        });
        const bytes = Buffer.from(response.data);
        const filename = `logs/${job.id}.log`;
        fs.writeFileSync(path.join(output, filename), bytes);
        record.raw_log = filename;
        record.raw_log_sha256 = sha(bytes);
      } catch (error) {
        unavailable = true;
        record.raw_log_unavailable = { status: error.status ?? null };
      }
    } else {
      record.raw_log_unavailable = { reason: 'job has no completed original log yet' };
    }
    records.push(record);
  }
  const fixture = fs.readFileSync('.github/round2/candidate.Cargo.lock');
  fs.writeFileSync(path.join(output, 'candidate.Cargo.lock'), fixture);
  const expectedLock = fs.readFileSync('.github/round2/candidate.Cargo.lock.sha256', 'utf8').split(/\s+/)[0];
  const files = {};
  const inputs = [];
  const mismatchedFiles = new Set();
  function scan(directory, inspectInputs = false) {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const filename = path.join(directory, entry.name);
      if (entry.isDirectory()) scan(filename, inspectInputs);
      else if (entry.isFile() && filename !== path.join(output, 'archive-files.sha256.json')) {
        const bytes = fs.readFileSync(filename);
        files[path.relative(output, filename)] = sha(bytes);
        if (!inspectInputs) continue;
        if (entry.name === 'checkout.json') {
          const proof = JSON.parse(bytes.toString('utf8'));
          inputs.push({ file: path.relative(output, filename), ...proof });
        }
        if (entry.name === 'used.Cargo.lock' && sha(bytes) !== expectedLock) {
          mismatchedFiles.add(path.relative(output, filename));
        }
        if (entry.name === 'source-verification.json') {
          const proof = JSON.parse(bytes.toString('utf8'));
          if (proof.expected_checkout !== candidate || proof.actual_checkout !== candidate
            || proof.changed_files.length) mismatchedFiles.add(path.relative(output, filename));
        }
      }
    }
  }
  scan(output, true);
  const mismatched = inputs.filter(input => input.checkout_sha !== candidate
    || input['candidate.Cargo.lock_sha256'] !== expectedLock);
  const record = { candidate_sha: candidate, run_id: context.runId,
    collector_node: process.version,
    run_url: `${context.serverUrl}/${context.repo.owner}/${context.repo.repo}/actions/runs/${context.runId}`,
    'candidate.Cargo.lock_sha256': sha(fixture), expected_lock_sha256: expectedLock,
    scheduled_checks: JSON.parse(process.env.ROUND2_CHECK_RESULTS ?? '{}'),
    jobs: records, inputs, mismatched_inputs: [...mismatched.map(input => input.file), ...mismatchedFiles],
    qualification: 'actual per-job results; skipped/cancelled/missing checks grant no qualification' };
  fs.writeFileSync(path.join(output, 'workflow-results.json'), JSON.stringify(record, null, 2) + '\n');
  scan(output);
  fs.writeFileSync(path.join(output, 'archive-files.sha256.json'), JSON.stringify(files, null, 2) + '\n');
  if (unavailable || inputs.length === 0 || mismatched.length || mismatchedFiles.size || sha(fixture) !== expectedLock) {
    core.setFailed('Round2 evidence is incomplete or does not match the exact candidate source/lock');
  }
};
