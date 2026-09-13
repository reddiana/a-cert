// Obsidian "Advanced Merger" 플러그인(.obsidian/plugins/advanced-merger)의 병합 결과를 재현한다.
// 플러그인 설정: sortMode=logical, includeNestedFolders=true, includeFoldersAsSections=true,
//               includeFilenames=true, removeYamlProperties=false
//
// 사용법: node tools/merge-spec.mjs <vault 기준 폴더> <출력 md>
//   예)   node tools/merge-spec.mjs ArchitectureSpecification .export/ArchitectureSpecification-merged.md
import fs from 'node:fs';
import path from 'node:path';

const [srcDir, outFile] = process.argv.slice(2);
if (!srcDir || !outFile) {
  console.error('usage: node merge-spec.mjs <folder> <out.md>');
  process.exit(1);
}

// 플러그인은 vault 루트 기준 경로(예: "ArchitectureSpecification/1. Project Overview")로 정렬·헤딩 깊이를 계산한다.
const vaultRoot = path.dirname(path.resolve(srcDir));
const rel = (p) => path.relative(vaultRoot, p).split(path.sep).join('/');

const items = [];
(function walk(dir) {
  for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
    if (ent.name.startsWith('.')) continue; // Obsidian 은 dot 폴더/파일을 vault 에서 제외
    const full = path.join(dir, ent.name);
    if (ent.isDirectory()) {
      items.push({ type: 'folder', full, name: ent.name, path: rel(full) });
      walk(full);
    } else if (ent.name.endsWith('.md')) {
      items.push({ type: 'file', full, name: ent.name, path: rel(full) });
    }
  }
})(path.resolve(srcDir));

// sortMode "logical": path.localeCompare(other, undefined, {numeric, sensitivity: 'base'})
const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base', usage: 'sort' });
items.sort((a, b) => collator.compare(a.path, b.path));

let out = '';
items.forEach((item, i) => {
  const depth = (item.path.match(/\//g) || []).length;
  const isLast = i === items.length - 1;
  let g = i === 0 ? '' : '\n';
  if (item.type === 'file') {
    g += `${'#'.repeat(depth)} ${item.name.replace(/\.md$/, '')}\n\n`;
    g += fs.readFileSync(item.full, 'utf8');
    if (!isLast) g += '\n\n';
  } else {
    // 플러그인은 폴더 헤딩의 '#' 뒤에 공백을 넣지 않는다 (후처리 sed 의 "#1 -> # 1" 보정 대상)
    g += '#'.repeat(depth) + item.name + '\n\n';
  }
  out += g;
});

fs.mkdirSync(path.dirname(path.resolve(outFile)), { recursive: true });
fs.writeFileSync(outFile, out);
console.log(`merged ${items.filter((x) => x.type === 'file').length} files -> ${outFile}`);
