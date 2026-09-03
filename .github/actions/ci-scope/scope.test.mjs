import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { scopeForEvent, selectScope } from './scope.mjs';

const full = { code: true, compatibility: true, tun: true };
const cheap = { code: false, compatibility: false, tun: false };
const base = 'a'.repeat(40);
const head = 'b'.repeat(40);

test('documentation-only changes skip Rust jobs', () => {
  assert.deepEqual(selectScope(['README.md', 'docs/project/tooling.md', 'LICENSE']), cheap);
});

test('every production crate and protocol retains TUN coverage', () => {
  for (const path of [
    'crates/tun/src/route.rs', 'crates/config/src/lib.rs',
    'crates/proxy/src/runtime/tcp_dispatch.rs', 'crates/platform/tokio/src/egress.rs',
    'crates/engine/src/runtime.rs', 'crates/traits/src/lib.rs',
    'protocols/vless/src/lib.rs', 'src/application/tun.rs',
  ]) assert.equal(selectScope([path]).tun, true, path);
});

test('domain changes retain ordinary tests without an unrelated compatibility build', () => {
  assert.deepEqual(selectScope(['crates/router/src/lib.rs']), {
    code: true, compatibility: false, tun: true,
  });
});

test('build, dependency, workflow and native changes require full coverage', () => {
  for (const path of [
    'Cargo.toml', 'Cargo.lock', 'crates/dns/Cargo.toml', 'protocols/http/build.rs',
    'Cross.toml', 'rust-toolchain.toml', '.cargo/config.toml',
    '.github/workflows/ci.yml', '.github/actions/ci-scope/scope.mjs',
    'scripts/prepare-wintun.ps1', 'crates/platform/tokio/src/lib.rs',
    'crates/transport/src/tls.rs', 'crates/ztls/src/lib.rs',
  ]) assert.deepEqual(selectScope([path]), full, path);
});

test('TUN test edits keep the privileged gate', () => {
  for (const path of ['tests/tun_privileged_e2e.rs', 'tests/tun_route_reconcile_macos_e2e.rs']) {
    assert.deepEqual(selectScope([path]), { code: true, compatibility: false, tun: true });
  }
});

test('examples and proto edits cannot bypass ordinary Rust tests', () => {
  assert.equal(selectScope(['examples/client.json']).code, true);
  assert.equal(selectScope(['proto/zero.proto']).code, true);
  assert.equal(selectScope(['proto/zero.proto']).tun, true);
});

test('ordinary test-only changes do not start privileged or compatibility jobs', () => {
  assert.deepEqual(selectScope(['tests/status.rs']), {
    code: true, compatibility: false, tun: false,
  });
});

test('manual and main qualification always run everything', () => {
  const noGit = () => assert.fail('full qualification must not depend on a diff');
  assert.deepEqual(scopeForEvent('workflow_dispatch', {}, noGit), full);
  assert.deepEqual(scopeForEvent('push', { ref: 'refs/heads/main' }, noGit), full);
  assert.deepEqual(scopeForEvent('pull_request', { pull_request: { base: { ref: 'main' } } }, noGit), full);
});

test('push uses the entire before/after diff including old paths of renamed files', () => {
  const calls = [];
  const scope = scopeForEvent('push', { before: base, after: head }, args => {
    calls.push(args);
    return 'crates/tun/src/old.rs\0docs/old.md\0';
  });
  assert.deepEqual(calls, [['diff', '--name-only', '--no-renames', '-z', base, head, '--']]);
  assert.deepEqual(scope, full);
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
  assert.deepEqual(scopeForEvent('push', { before: base, after: head }, () => files.join('\0')), full);
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
  assert.doesNotMatch(ci, /cargo check --workspace --all-features/);
  assert.doesNotMatch(ci, /cargo test -p zero-proxy --test runtime_boundary/);
  assert.match(ci, /if: needs.scope.outputs.compatibility == 'true'\s+run: cargo test --test tun_privileged_e2e --no-run/);
  assert.doesNotMatch(tun, /sudo[^\n]*cargo test/);
  assert.match(tun, /CARGO_TARGET_X86_64_APPLE_DARWIN_RUNNER: sudo/);
  assert.equal(tun.match(/cache-on-failure: true/g)?.length, 3);
  assert.match(tun, /cargo test --target x86_64-apple-darwin/);
  assert.match(tun, /privileged_windows_ipv4_only_tun_falls_back_trusted_ipv6_domains/);
  assert.doesNotMatch(tun, /if: github\.event_name != 'pull_request'\r?\n/);
  assert.doesNotMatch(tun, /continue-on-error:/);
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
