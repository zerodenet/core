import { execFileSync } from 'node:child_process';
import { appendFileSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const fullScope = () => ({ code: true, compatibility: true, tun: true });
const startsWithAny = (path, prefixes) => prefixes.some(prefix => path.startsWith(prefix));

export function selectScope(paths) {
  // Only known documentation is cheap. Examples and proto files can be compiled
  // or consumed by tests, so they must not bypass the ordinary Rust checks.
  const relevant = paths.filter(path => !(
    path.endsWith('.md') || path === 'LICENSE' || path.startsWith('docs/')
  ));
  if (relevant.length === 0) {
    return { code: false, compatibility: false, tun: false };
  }

  const buildChanged = relevant.some(path =>
    /(^|\/)(Cargo\.(toml|lock)|build\.rs|Cross\.toml|rust-toolchain(\.toml)?)$/.test(path)
    || startsWithAny(path, ['.cargo/', '.github/', 'scripts/'])
  );
  // Keep native/crypto/transport changes on the compatibility gate. Ordinary
  // domain logic is tested on Linux; main and manual runs always check all OSes.
  const compatibility = buildChanged || relevant.some(path => startsWithAny(path, [
    'src/', 'protocols/', 'crates/platform/', 'crates/tun/',
    'crates/transport/', 'crates/ztls/',
  ]));
  // Deliberately include all production crates: changes to config, routing,
  // shared traits or protocol dispatch can affect TUN without editing tun/.
  const tun = buildChanged || relevant.some(path =>
    startsWithAny(path, ['src/', 'crates/', 'protocols/', 'proto/', 'tests/tun'])
  );
  return { code: true, compatibility, tun };
}

export function scopeForEvent(eventName, event, git) {
  // RC/main candidates and explicit manual qualification must never be filtered.
  if (eventName === 'workflow_dispatch' || event.ref === 'refs/heads/main'
      || event.pull_request?.base?.ref === 'main') {
    return fullScope();
  }

  try {
    let base;
    let head;
    if (eventName === 'pull_request') {
      base = event.pull_request?.base?.sha;
      head = event.pull_request?.head?.sha;
    } else if (eventName === 'push') {
      base = event.before;
      head = event.after;
    }
    const validSha = value => typeof value === 'string'
      && /^[a-f0-9]{40}$/.test(value) && !/^0+$/.test(value);
    if (!validSha(base) || !validSha(head)) return fullScope();
    if (eventName === 'pull_request') {
      base = git(['merge-base', base, head]).trim();
      if (!validSha(base)) return fullScope();
    }
    // --no-renames includes both sides of moves; -z preserves unusual filenames.
    // Local Git avoids the API's changed-file pagination/truncation limits.
    const paths = git(['diff', '--name-only', '--no-renames', '-z', base, head, '--'])
      .split('\0').filter(Boolean);
    return selectScope(paths);
  } catch {
    // Force pushes and missing history must run more checks, never skip them.
    console.warn('Changed-file detection failed; running the full CI scope.');
    return fullScope();
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const event = JSON.parse(readFileSync(process.env.GITHUB_EVENT_PATH, 'utf8'));
  const scope = scopeForEvent(process.env.GITHUB_EVENT_NAME, event, args =>
    execFileSync('git', args, { encoding: 'utf8', maxBuffer: 16 * 1024 * 1024 })
  );
  for (const [name, enabled] of Object.entries(scope)) {
    appendFileSync(process.env.GITHUB_OUTPUT, `${name}=${enabled}\n`);
  }
  console.log(JSON.stringify(scope));
}
