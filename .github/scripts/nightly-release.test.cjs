const {test} = require('node:test');
const assert = require('node:assert/strict');
const {completeRelease, shouldBuild, requireCI} = require('./nightly-release.cjs');
const sha = 'a'.repeat(40);
const candidate = {branch: 'develop', sha};
const success = {id: 8, head_sha: sha, head_branch: 'develop', event: 'workflow_dispatch',
  status: 'completed', conclusion: 'success', html_url: 'https://example.invalid/ci/8'};

function fixture(responses, branchSha = sha) {
  const calls = [];
  const api = {context: {repo: {owner: 'test', repo: 'test'}}, core: {info() {}},
    github: {rest: {repos: {getBranch: async () => ({data: {commit: {sha: branchSha}}})},
      actions: {listWorkflowRuns() {}, createWorkflowDispatch: async args => calls.push(args)}},
    paginate: async () => responses.shift() || []}};
  let time = 0;
  return {api, calls, options: {now: () => time, sleep: async ms => {time += ms;}, timeout: 90000}};
}

test('reuse only complete-scope successful CI on the exact branch and SHA', async () => {
  const f = fixture([[success]]);
  await requireCI(f.api, candidate, f.options);
  assert.equal(f.calls.length, 0);
});

test('ordinary push success and unrelated CI cannot satisfy nightly qualification', async () => {
  const f = fixture([[{...success, event: 'push'}, {...success, head_sha: 'b'.repeat(40)},
    {...success, head_branch: 'main'}], [], [success]]);
  await requireCI(f.api, candidate, f.options);
  assert.equal(f.calls.length, 1);
  assert.equal(f.calls[0].ref, 'develop');
  assert.equal(f.calls[0].workflow_id, 'ci.yml');
});

test('failed CI can retry, but a new failed run stops before tagging', async () => {
  const old = {...success, conclusion: 'failure'};
  const f = fixture([[old], [old], [{...old, id: 9}]]);
  await assert.rejects(requireCI(f.api, candidate, f.options), /CI failure/);
  assert.equal(f.calls.length, 1);
});

test('branch movement before dispatch rejects a floating ref', async () => {
  const f = fixture([[]], 'b'.repeat(40));
  await assert.rejects(requireCI(f.api, candidate, f.options), /Branch advanced/);
  assert.equal(f.calls.length, 0);
});

test('missing run, cancellation and stale success never release a candidate', async () => {
  for (const responses of [[[], [], []], [[{...success, conclusion: 'failure'}], [success]],
    [[], [{...success, conclusion: 'cancelled'}]]]) {
    const f = fixture(responses);
    await assert.rejects(requireCI(f.api, candidate, f.options), /deadline|CI cancelled/);
  }
});

test('invalid candidate identity fails before any API calls', async () => {
  for (const bad of [{branch: 'feature', sha}, {branch: 'main', sha: 'bad'}]) {
    const f = fixture([]);
    await assert.rejects(requireCI(f.api, bad, f.options), /Invalid candidate/);
    assert.equal(f.calls.length, 0);
  }
});

const assets = ['linux-x86_64.tar.gz', 'linux-x86_64-musl.tar.gz', 'darwin-x86_64.tar.gz',
  'darwin-aarch64.tar.gz', 'windows-x86_64.zip'].flatMap(name =>
  [{name: `zero-${name}`, size: 100}, {name: `zero-${name}.sha256`, size: 64}]);

test('skip only published prerelease with all archives and checksums', async () => {
  const complete = {draft: false, prerelease: true, assets};
  assert.equal(completeRelease(complete), true);
  for (const incomplete of [{...complete, draft: true}, {...complete, prerelease: false},
    {...complete, assets: assets.slice(1)}, {...complete, assets: [{...assets[0], size: 0}, ...assets.slice(1)]}]) {
    assert.equal(completeRelease(incomplete), false);
  }
  const api = {context: {repo: {}}, core: {info() {}}, github: {rest: {repos: {
    getReleaseByTag: async () => ({data: complete}),
  }, actions: {listWorkflowRuns() {}}}, paginate: async () => []}};
  assert.equal(await shouldBuild(api, {mode: 'existing', tag: 'v0.0.3-dev.202610071200'}), false);
  assert.equal(await shouldBuild(api, {mode: 'prepare'}), true);
  assert.equal(await shouldBuild(api, {mode: 'retry'}), true);
  assert.equal(await shouldBuild(api, {mode: 'skip'}), false);
  api.github.rest.repos.getReleaseByTag = async () => {throw {status: 404};};
  assert.equal(await shouldBuild(api, {mode: 'existing'}), true);
  api.github.rest.repos.getReleaseByTag = async () => {throw {status: 403};};
  await assert.rejects(shouldBuild(api, {mode: 'existing'}));
});

test('an active build of the same tag is not dispatched twice', async () => {
  const api = {context: {repo: {}}, core: {info() {}}, github: {rest: {
    repos: {getReleaseByTag: async () => {throw {status: 404};}},
    actions: {listWorkflowRuns() {}},
  }, paginate: async () => [{display_title: 'Release v0.0.3-dev.202610071200', status: 'in_progress'}]}};
  assert.equal(await shouldBuild(api, {mode: 'existing', tag: 'v0.0.3-dev.202610071200'}), false);
  assert.equal(await shouldBuild(api, {mode: 'existing', tag: 'v0.0.3-dev.202610091917'}), true);
});
