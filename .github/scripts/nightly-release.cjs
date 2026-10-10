// Control-plane helpers: no tag is created until the exact commit passes CI.
const archives = ['linux-x86_64.tar.gz', 'linux-x86_64-musl.tar.gz',
  'darwin-x86_64.tar.gz', 'darwin-aarch64.tar.gz', 'windows-x86_64.zip'];
const expectedAssets = archives.flatMap(name => [`zero-${name}`, `zero-${name}.sha256`]);

function completeRelease(release) {
  const assets = new Map(release.assets.map(asset => [asset.name, asset.size]));
  return !release.draft && release.prerelease &&
    expectedAssets.every(name => assets.get(name) > 0);
}

async function shouldBuild({github, context, core}, plan) {
  if (plan.mode === 'skip') return false;
  if (plan.mode !== 'existing') return true;
  try {
    const {data} = await github.rest.repos.getReleaseByTag({...context.repo, tag: plan.tag});
    if (completeRelease(data)) {
      core.info(`Skipping ${plan.branch}: ${plan.tag} is already published, no new commits`);
      return false;
    }
  } catch (error) {
    if (error.status !== 404) throw error;
  }
  const runs = await github.paginate(github.rest.actions.listWorkflowRuns, {
    ...context.repo, workflow_id: 'release.yml', per_page: 100,
  });
  if (runs.some(run => run.display_title === `Release ${plan.tag}` &&
    ['queued', 'in_progress', 'waiting', 'pending', 'requested'].includes(run.status))) {
    core.info(`Release ${plan.tag} is already building; no duplicate dispatch`);
    return false;
  }
  core.info(`Retrying incomplete release ${plan.tag} without moving its tag`);
  return true;
}

async function requireCI({github, context, core}, candidate, {
  sleep = ms => new Promise(resolve => setTimeout(resolve, ms)),
  now = () => Date.now(), timeout = 70 * 60 * 1000,
} = {}) {
  const {branch, sha} = candidate;
  if (!['main', 'develop'].includes(branch) || !/^[0-9a-f]{40}$/.test(sha)) {
    throw new Error('Invalid candidate identity');
  }
  const list = () => github.paginate(github.rest.actions.listWorkflowRuns, {
    ...context.repo, workflow_id: 'ci.yml', head_sha: sha, branch, per_page: 100,
  });
  const matches = run => run.head_sha === sha && run.head_branch === branch &&
    run.event === 'workflow_dispatch'; // Always qualify the complete CI scope.
  const previous = (await list()).filter(matches).sort((a, b) => b.id - a.id);
  if (previous[0]?.status === 'completed' && previous[0].conclusion === 'success') {
    core.info(`Reusing full CI: ${previous[0].html_url}`);
    return;
  }
  const {data} = await github.rest.repos.getBranch({...context.repo, branch});
  if (data.commit.sha !== sha) throw new Error('Branch advanced before CI dispatch; retry next run');
  await github.rest.actions.createWorkflowDispatch({...context.repo,
    workflow_id: 'ci.yml', ref: branch});
  const previousId = previous[0]?.id || 0;
  const deadline = now() + timeout;
  let previousState;
  while (now() < deadline) {
    const run = (await list()).filter(run => matches(run) && run.id > previousId)
      .sort((a, b) => b.id - a.id)[0];
    if (run?.status === 'completed') {
      if (run.conclusion !== 'success') throw new Error(`CI ${run.conclusion}: ${run.html_url}`);
      core.info(`Full CI passed: ${run.html_url}`);
      return;
    }
    const state = run ? `${run.id}: ${run.status}` : 'dispatch not yet visible';
    if (state !== previousState) { core.info(state); previousState = state; }
    await sleep(30000);
  }
  throw new Error(`No successful full CI for ${branch}@${sha} within the deadline`);
}

module.exports = {completeRelease, shouldBuild, requireCI};
