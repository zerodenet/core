const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const crypto = require('node:crypto');
const collect = require('./collect-round2-evidence.cjs');
const sha = bytes => crypto.createHash('sha256').update(bytes).digest('hex');

test('exact run collection keeps original bytes, skipped state and rejects unknown input/lock/logs', async (t) => {
  t.diagnostic(`collector harness Node ${process.version}; isolated fixtures grant no Rust qualification`);
  const original = process.cwd();
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'zero-evidence-collector-'));
  const lock = Buffer.from('isolated collector fixture; not a Cargo qualification\n');
  const raw = Buffer.from([0, 255, 10, 27, 91, 51, 49, 109, 65]);
  try {
    for (const mode of ['matching', 'foreign-source', 'foreign-used-lock', 'changed-source', 'missing-log', 'missing-input']) {
      const directory = path.join(scratch, mode);
      fs.mkdirSync(directory);
      process.chdir(directory);
      fs.mkdirSync('.github/round2', { recursive: true });
      fs.writeFileSync('.github/round2/candidate.Cargo.lock', lock);
      fs.writeFileSync('.github/round2/candidate.Cargo.lock.sha256', sha(lock) + '  candidate.Cargo.lock\n');
      const input = '.round2-evidence/collected/inputs/one-exact-job';
      fs.mkdirSync(input, { recursive: true });
      if (mode !== 'missing-input') {
        fs.writeFileSync(path.join(input, 'checkout.json'), JSON.stringify({
          checkout_sha: mode === 'foreign-source' ? 'other-input' : 'candidate-input',
          'candidate.Cargo.lock_sha256': sha(lock),
        }));
        fs.writeFileSync(path.join(input, 'used.Cargo.lock'), mode === 'foreign-used-lock' ? 'different used bytes' : lock);
        fs.writeFileSync(path.join(input, 'source-verification.json'), JSON.stringify({
          expected_checkout: 'candidate-input', actual_checkout: 'candidate-input',
          changed_files: mode === 'changed-source' ? ['actual-changed-source.rs'] : [],
        }));
      }
      const requested = [];
      const failures = [];
      const github = {
        paginate: async () => [
          { id: 7, name: 'isolated failed-command fixture', status: 'completed', conclusion: 'failure', started_at: 'start' },
          { id: 8, name: 'not executed', status: 'completed', conclusion: 'skipped', started_at: 'start' },
          { id: 9, name: 'collector itself', status: 'in_progress', conclusion: null },
        ],
        rest: { actions: { listJobsForWorkflowRun: () => {},
          downloadJobLogsForWorkflowRun: async ({ job_id }) => {
            requested.push(job_id);
            if (mode === 'missing-log') throw Object.assign(new Error('do not serialize request credentials'), { status: 404 });
            return { data: raw };
          },
        } },
      };
      await collect({ github,
        context: { payload: { pull_request: { head: { sha: 'candidate-input' } } },
          sha: 'unused-merge-input', repo: { owner: 'fixture', repo: 'fixture' }, runId: 10, serverUrl: 'https://github.com' },
        core: { setFailed: message => failures.push(message) },
      });
      assert.deepEqual(requested, [7]);
      assert.equal(failures.length, mode === 'matching' ? 0 : 1, mode);
      const result = JSON.parse(fs.readFileSync('.round2-evidence/collected/workflow-results.json'));
      assert.equal(result.candidate_sha, 'candidate-input');
      assert.equal(result.jobs[0].conclusion, 'failure');
      assert.equal(result.jobs[1].raw_log_unavailable.reason, 'job was not executed');
      if (mode !== 'missing-log') {
        assert.deepEqual(fs.readFileSync('.round2-evidence/collected/logs/7.log'), raw);
        assert.equal(result.jobs[0].raw_log_sha256, sha(raw));
      }
      const manifest = JSON.parse(fs.readFileSync('.round2-evidence/collected/archive-files.sha256.json'));
      assert.equal(manifest['workflow-results.json'], sha(fs.readFileSync('.round2-evidence/collected/workflow-results.json')));
    }
  } finally {
    process.chdir(original);
    fs.rmSync(scratch, { recursive: true, force: true });
  }
});
