import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { scopeForEvent, selectScope } from './scope.mjs';

const full = { code: true, compatibility: true, tun: true, exhaustive: true };
const selectedFull = { code: true, compatibility: true, tun: true, exhaustive: false };
const cheap = { code: false, compatibility: false, tun: false, exhaustive: false };
const base = 'a'.repeat(40);
const head = 'b'.repeat(40);

test('documentation-only changes skip Rust jobs', () => {
  assert.deepEqual(selectScope(['README.md', 'docs/project/tooling.md', 'LICENSE']), cheap);
});

test('every TUN-participating layer retains privileged coverage', () => {
  for (const path of [
    'crates/tun/src/route.rs', 'crates/config/src/lib.rs',
    'crates/proxy/src/runtime/tcp_dispatch.rs', 'crates/platform/tokio/src/egress.rs',
    'crates/router/src/lib.rs', 'crates/stack/src/lib.rs', 'crates/traits/src/lib.rs',
    'protocols/vless/src/lib.rs', 'src/application/tun.rs',
  ]) assert.equal(selectScope([path]).tun, true, path);
});

test('ordinary application and engine changes stay on the Linux quality gate', () => {
  for (const path of ['src/application/run.rs', 'crates/engine/src/runtime.rs']) {
    assert.deepEqual(selectScope([path]), {
      code: true, compatibility: false, tun: false, exhaustive: false,
    }, path);
  }
});

test('domain changes retain ordinary tests without an unrelated compatibility build', () => {
  assert.deepEqual(selectScope(['crates/router/src/lib.rs']), {
    code: true, compatibility: false, tun: true, exhaustive: false,
  });
});

test('build, dependency, workflow and native changes require full coverage', () => {
  for (const path of [
    'Cargo.toml', 'Cargo.lock', 'crates/dns/Cargo.toml', 'protocols/http/build.rs',
    'Cross.toml', 'rust-toolchain.toml', '.cargo/config.toml',
    '.github/workflows/ci.yml', '.github/actions/ci-scope/scope.mjs',
    'scripts/prepare-wintun.ps1', 'crates/platform/tokio/src/lib.rs',
    'crates/transport/src/tls.rs', 'crates/ztls/src/lib.rs',
    'vendor/quinn-proto/src/connection/pacing.rs', 'vendor/h3-quinn/src/lib.rs',
  ]) assert.deepEqual(selectScope([path]), selectedFull, path);
});

test('TUN test edits keep the privileged gate', () => {
  for (const path of [
    'tests/tun_privileged_e2e.rs',
    'tests/tun_route_reconcile_macos_e2e.rs',
    'scripts/capture-tun-windows.ps1',
  ]) {
    assert.deepEqual(selectScope([path]), {
      code: true, compatibility: false, tun: true, exhaustive: false,
    });
  }
});

test('examples and proto edits cannot bypass ordinary Rust tests', () => {
  assert.equal(selectScope(['examples/client.json']).code, true);
  assert.equal(selectScope(['proto/zero.proto']).code, true);
  assert.equal(selectScope(['proto/zero.proto']).tun, true);
});

test('ordinary test-only changes do not start privileged or compatibility jobs', () => {
  assert.deepEqual(selectScope(['tests/status.rs']), {
    code: true, compatibility: false, tun: false, exhaustive: false,
  });
});

test('offline validation keeps cross-platform lock coverage', () => {
  for (const path of [
    'tests/validate_isolation.rs', 'src/application/inspect.rs',
    'crates/proxy/src/validation.rs',
  ]) assert.equal(selectScope([path]).compatibility, true, path);
});

test('manual and scheduled qualification always run everything', () => {
  const noGit = () => assert.fail('full qualification must not depend on a diff');
  assert.deepEqual(scopeForEvent('workflow_dispatch', {}, noGit), full);
  assert.deepEqual(scopeForEvent('schedule', {}, noGit), full);
});

test('main pushes and pull requests use their actual diff', () => {
  assert.deepEqual(scopeForEvent('push', {
    ref: 'refs/heads/main', before: base, after: head,
  }, () => 'docs/project/tooling.md\0'), cheap);

  let calls = 0;
  const scope = scopeForEvent('pull_request', {
    pull_request: { base: { sha: base, ref: 'main' }, head: { sha: head } },
  }, () => {
    calls += 1;
    if (calls === 1) return `${'c'.repeat(40)}\n`;
    return 'src/application/run.rs\0';
  });
  assert.deepEqual(scope, {
    code: true, compatibility: false, tun: false, exhaustive: false,
  });
});

test('push uses the entire before/after diff including old paths of renamed files', () => {
  const calls = [];
  const scope = scopeForEvent('push', { before: base, after: head }, args => {
    calls.push(args);
    return 'crates/tun/src/old.rs\0docs/old.md\0';
  });
  assert.deepEqual(calls, [['diff', '--name-only', '--no-renames', '-z', base, head, '--']]);
  assert.deepEqual(scope, selectedFull);
});

test('PRs compare from the merge base, not unrelated base branch changes', () => {
  let calls = 0;
  const scope = scopeForEvent('pull_request', {
    pull_request: { base: { sha: base, ref: 'develop' }, head: { sha: head } },
  }, args => {
    calls += 1;
    if (calls === 1) {
      assert.deepEqual(args, ['merge-base', base, head]);
      return `${'c'.repeat(40)}\n`;
    }
    assert.equal(args[4], 'c'.repeat(40));
    return 'README.md\0';
  });
  assert.equal(calls, 2);
  assert.deepEqual(scope, cheap);
});

test('missing history, new branches and unknown events fail open to full verification', () => {
  const missing = () => { throw new Error('missing revision'); };
  assert.deepEqual(scopeForEvent('push', { before: base, after: head }, missing), full);
  assert.deepEqual(scopeForEvent('push', { before: '0'.repeat(40), after: head }, missing), full);
  assert.deepEqual(scopeForEvent('unknown', {}, missing), full);
});

test('large diffs and unusual filenames do not lose relevant paths', () => {
  const files = Array.from({ length: 3500 }, (_, i) => `docs/${i}.md`);
  files.push('crates/tun/src/file with spaces.rs');
  assert.deepEqual(
    scopeForEvent('push', { before: base, after: head }, () => files.join('\0')),
    selectedFull,
  );
});

test('workflow contracts preserve coverage and avoid root-owned build artifacts', () => {
  const ci = readFileSync('.github/workflows/ci.yml', 'utf8');
  const tun = readFileSync('.github/workflows/tun-e2e.yml', 'utf8');
  for (const workflow of [ci, tun]) {
    assert.doesNotMatch(workflow, /^\s+paths(-ignore)?:/m);
    assert.match(workflow, /fetch-depth: 0/);
    assert.match(workflow, /if: always\(\)/);
  }
  assert.match(ci, /cargo test --workspace --all-features/);
  assert.match(ci, /cargo clippy --workspace --all-targets --all-features/);
  assert.match(ci, /Check representative minimal feature surfaces/);
  assert.equal(ci.match(/if: needs\.scope\.outputs\.exhaustive == 'true'/g)?.length, 2);
  assert.doesNotMatch(ci, /cargo check --workspace --all-features/);
  assert.doesNotMatch(ci, /cargo test -p zero-proxy --test runtime_boundary/);
  assert.match(ci, /if: needs.scope.outputs.compatibility == 'true'\s+run: cargo test --test tun_privileged_e2e --no-run/);
  assert.doesNotMatch(tun, /sudo[^\n]*cargo test/);
  assert.match(tun, /CARGO_TARGET_X86_64_APPLE_DARWIN_RUNNER: sudo/);
  assert.equal(tun.match(/cache-on-failure: true/g)?.length, 3);
  assert.match(tun, /cargo test --target x86_64-apple-darwin/);
  assert.match(tun, /privileged_windows_ipv4_only_tun_falls_back_trusted_ipv6_domains/);
  assert.match(tun, /schedule:\s+- cron:/);
  assert.doesNotMatch(tun, /if: github\.event_name != 'pull_request'\r?\n/);
  assert.doesNotMatch(tun, /continue-on-error:/);
});

test('tag and artifact publishing reuse CI for the same commit', () => {
  const publish = readFileSync('.github/workflows/publish-release.yml', 'utf8');
  const release = readFileSync('.github/workflows/release.yml', 'utf8');
  assert.match(publish, /actions: read/);
  assert.match(publish, /workflows\/ci\.yml\/runs\?head_sha=\$GITHUB_SHA/);
  assert.doesNotMatch(publish, /cargo (fmt|clippy|test)/);
  assert.match(release, /actions: read/);
  assert.doesNotMatch(release, /cargo (fmt|clippy|test)/);
  assert.match(release, /workflow_id: 'ci.yml', head_sha: sha/);
  assert.match(release, /CI_SHA: \$\{\{ needs.guard.outputs.sha \}\}/);
  assert.match(release, /needs: \[guard, build\]/);
  assert.match(release, /Compile alongside branch CI[^]*?needs: guard/);
  assert.ok(release.indexOf('Require successful CI for the release commit') < release.indexOf('Create release\n'));
});

test('both result gates propagate failures and cancellations but accept intentional skips', () => {
  for (const file of ['ci.yml', 'tun-e2e.yml']) {
    const workflow = readFileSync(`.github/workflows/${file}`, 'utf8');
    const script = workflow.match(/node -e '([^']+)'/)[1];
    for (const [result, exitCode] of [
      ['success', 0], ['skipped', 0], ['failure', 1], ['cancelled', 1],
    ]) {
      const child = spawnSync(process.execPath, ['-e', script], {
        env: {
          ...process.env,
          CHECK_RESULTS: JSON.stringify({ scope: { result: 'success' }, selected: { result } }),
        },
        encoding: 'utf8',
      });
      assert.equal(child.status, exitCode, `${file}: ${result}: ${child.stderr}`);
    }
  }
});

test('release gate accepts only successful CI for its exact source commit', async () => {
  const workflow = readFileSync('.github/workflows/release.yml', 'utf8');
  const source = workflow.match(/          script: \|\n((?:            [^\n]*\n)+)/)[1]
    .split('\n').map(line => line.slice(12)).join('\n');
  const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
  const gate = new AsyncFunction('github', 'context', 'core', 'process', 'Date', 'setTimeout', source);
  const sha = 'a'.repeat(40);
  const success = { id: 1, head_sha: sha, head_branch: 'develop', event: 'push',
    status: 'completed', conclusion: 'success', html_url: 'https://example.invalid/ci/1' };
  async function run(snapshots, { env = { CI_SHA: sha, CI_BRANCH: 'develop' }, apiError } = {}) {
    let now = 0;
    let calls = 0;
    const requests = [];
    const github = {
      rest: { actions: { listWorkflowRuns: {} } },
      paginate: async (_method, request) => {
        if (apiError) throw apiError;
        requests.push(request);
        return snapshots[Math.min(calls++, snapshots.length - 1)];
      },
    };
    await gate(github, { repo: { owner: 'zerodenet', repo: 'core' } }, { info() {} },
      { env }, { now: () => now }, resolve => { now += 20 * 60 * 1000; resolve(); });
    return requests;
  }
  const requests = await run([[success]]);
  assert.equal(requests[0].head_sha, sha);
  assert.equal(requests[0].branch, 'develop');
  assert.equal(requests[0].workflow_id, 'ci.yml');
  assert.equal((await run([[], [{ ...success, status: 'in_progress' }], [success]])).length, 3);
  for (const conclusion of ['failure', 'cancelled', 'timed_out', 'skipped', 'neutral']) {
    await assert.rejects(run([[{ ...success, conclusion }]]), /Release blocked/);
  }
  for (const override of [{ head_sha: 'b'.repeat(40) }, { head_branch: 'main' },
    { event: 'pull_request' }, { event: 'schedule' }]) {
    await assert.rejects(run([[{ ...success, ...override }]]), /No successful CI/);
  }
  // A newer run must supersede an older success, including while it is pending.
  await assert.rejects(run([[success, { ...success, id: 2, conclusion: 'failure' }]]), /Release blocked/);
  await assert.rejects(run([[success, { ...success, id: 2, status: 'queued' }]]), /No successful CI/);
  await run([[{ ...success, event: 'workflow_dispatch' }]]);
  await run([[{ ...success, head_branch: 'main' }]], { env: { CI_SHA: sha, CI_BRANCH: 'main' } });
  await assert.rejects(run([[]]), /No successful CI/);
  await assert.rejects(run([[success]], { env: {} }), /invalid release commit identity/);
  await assert.rejects(run([[success]], { apiError: new Error('API unavailable') }), /API unavailable/);
});
