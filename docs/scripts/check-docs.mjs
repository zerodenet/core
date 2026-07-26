import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { dirname, extname, join, relative, resolve } from 'node:path';

const root = resolve(import.meta.dirname, '..');
const configPath = resolve(root, '.vitepress/config.ts');
const ignoredDirectories = new Set(['.vitepress', 'node_modules']);
const checkDist = process.argv.includes('--dist');

function walk(directory) {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    if (entry.isDirectory() && ignoredDirectories.has(entry.name)) return [];
    const path = join(directory, entry.name);
    return entry.isDirectory() ? walk(path) : [path];
  });
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function routeForMarkdown(file) {
  const path = relative(root, file).replaceAll('\\', '/');
  if (path === 'index.md') return '/';
  if (path.endsWith('/index.md')) return `/${path.slice(0, -'index.md'.length)}`;
  return `/${path.slice(0, -'.md'.length)}`;
}

function localTargetExists(source, rawTarget) {
  const withoutAnchor = rawTarget.split('#')[0].split('?')[0];
  if (!withoutAnchor) return true;

  const target = withoutAnchor.startsWith('/')
    ? join(root, withoutAnchor)
    : resolve(dirname(source), withoutAnchor);

  if (existsSync(target) && extname(target)) return true;
  if (existsSync(`${target}.md`)) return true;
  if (existsSync(join(target, 'index.md'))) return true;
  return existsSync(target) && !withoutAnchor.endsWith('/');
}

const markdownFiles = walk(root).filter((path) => path.endsWith('.md'));
const config = readFileSync(configPath, 'utf8');
const mojibakePatterns = ['锛', '〆', '鈹', '駃', '\uFFFD', 'Ã', 'Â'];
const brokenLinks = [];
const htmlLinks = [];
const routesWithIncomingLinks = new Set(['/']);

for (const file of markdownFiles) {
  const content = readFileSync(file, 'utf8');
  const displayPath = relative(root, file).replaceAll('\\', '/');
  assert(
    content.trimStart().startsWith('#') || content.trimStart().startsWith('---'),
    `${displayPath}: 文档必须以标题或 frontmatter 开头`,
  );

  for (const pattern of mojibakePatterns) {
    assert(!content.includes(pattern), `${displayPath}: 检测到疑似乱码: ${pattern}`);
  }

  for (const match of content.matchAll(/\[[^\]]*]\(([^)]+)\)/g)) {
    const target = match[1].trim().replace(/^<|>$/g, '');
    if (!target || /^(https?:|mailto:|#)/.test(target)) continue;
    if (target.split(/[?#]/)[0].endsWith('.html')) htmlLinks.push(`${displayPath}: ${target}`);
    if (!localTargetExists(file, target)) brokenLinks.push(`${displayPath}: ${target}`);

    const withoutAnchor = target.split('#')[0].split('?')[0];
    const resolved = withoutAnchor.startsWith('/')
      ? resolve(root, `.${withoutAnchor}`)
      : resolve(dirname(file), withoutAnchor);
    const candidates = [resolved, `${resolved}.md`, join(resolved, 'index.md')];
    const linkedMarkdown = candidates.find((candidate) => candidate.endsWith('.md') && existsSync(candidate));
    if (linkedMarkdown) routesWithIncomingLinks.add(routeForMarkdown(linkedMarkdown));
  }
}

assert(brokenLinks.length === 0, `发现失效的本地链接:\n${brokenLinks.join('\n')}`);
assert(htmlLinks.length === 0, `内部链接不应写死 .html:\n${htmlLinks.join('\n')}`);

const duplicateIndexes = readdirSync(root, { withFileTypes: true })
  .filter((entry) => entry.isDirectory() && !ignoredDirectories.has(entry.name))
  .filter((entry) => existsSync(join(root, entry.name, 'index.md')) && existsSync(join(root, entry.name, 'README.md')))
  .map((entry) => entry.name);
assert(duplicateIndexes.length === 0, `目录同时存在 index.md 和 README.md: ${duplicateIndexes.join(', ')}`);

assert(!config.includes('readdirSync'), '导航不得根据文件系统自动生成');
assert(!config.includes('readFileSync'), '导航不得根据 Markdown 标题自动命名');
assert(!config.includes("text: '控制平面 API'"), '导航不得恢复独立的“控制平面 API”菜单');
assert(!config.includes("text: '控制平面'"), '导航不得恢复与“控制与集成”并列的语义');

const unindexed = markdownFiles
  .filter((file) => relative(root, file).replaceAll('\\', '/') !== 'README.md')
  .map((file) => routeForMarkdown(file))
  .filter((route) => route !== '/' && !config.includes(`'${route}'`) && !routesWithIncomingLinks.has(route));
assert(unindexed.length === 0, `文档未进入导航或索引:\n${unindexed.join('\n')}`);

assert(readFileSync(resolve(root, 'README.md'), 'utf8').includes('历史设计与方案背景'), '根 README 必须说明历史控制面目录的定位');
assert(readFileSync(resolve(root, 'protocols/index.md'), 'utf8').includes('协议概览'), '协议入口必须保留中文标题');
assert(!config.includes('/protocols/http-connect/'), '导航包含失效的 HTTP CONNECT 路径');

if (checkDist) {
  const source = readFileSync(resolve(root, 'project/tooling.md'), 'utf8');
  const htmlPath = resolve(root, '.vitepress/dist/project/tooling.html');
  assert(existsSync(htmlPath), '站点构建产物缺少 project/tooling.html');
  const html = readFileSync(htmlPath, 'utf8');
  for (const content of [source, html]) {
    assert(content.includes('工程规则'), '构建产物缺少 UTF-8 中文标题');
    assert(content.includes('status-api'), '构建产物缺少当前 feature: status-api');
    assert(content.includes('event-dispatcher'), '构建产物缺少当前 feature: event-dispatcher');
  }
}

console.log(`文档检查通过：${markdownFiles.length} 个 Markdown 文件，路径、索引、编码和导航约束正常`);
