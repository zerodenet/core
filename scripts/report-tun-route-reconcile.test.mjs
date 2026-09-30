import assert from 'node:assert/strict';
import test from 'node:test';
import { classifyCoverage } from './report-tun-route-reconcile.mjs';

const evidence = (status) => ({ scenario: 'windows-route-switch', status, executed: status === 'passed', reason: 'fixture result' });

test('missing interface is visible as skipped without claiming execution', () => {
  const result = classifyCoverage(evidence('skipped'), 0);
  assert.equal(result.status, 'skipped');
  assert.equal(result.level, 'warning');
  assert.match(result.message, /no network-switch coverage/);
});

test('successful executed scenario is distinct from skip', () => {
  assert.equal(classifyCoverage(evidence('passed'), 0).status, 'passed');
});

test('failed cargo or absent, stale, and inconsistent evidence cannot pass', () => {
  for (const report of [undefined, {}, { ...evidence('passed'), scenario: 'other' }, { ...evidence('skipped'), executed: true }, { ...evidence('passed'), reason: '' }]) {
    assert.throws(() => classifyCoverage(report, 0));
  }
  assert.throws(() => classifyCoverage(evidence('passed'), 101));
  assert.throws(() => classifyCoverage(evidence('passed'), NaN));
});
