// draw.io SVG 는 글자를 HTML(<foreignObject>)로 담고 있어 Word·rsvg-convert 에서는 글자가 사라진다.
// headless Chrome(puppeteer)으로 PNG 렌더링한 뒤 병합 md 의 SVG 이미지 참조를 PNG 로 바꾼다.
//
// 사용법: PUPPETEER_MODULE=<puppeteer 경로> node tools/svg2png.mjs <병합.md> <출력 폴더(절대경로)> <리소스 폴더...>
//   - 대상: ![alt](<경로.svg>) 형식 (export-docx.sh 후처리 결과)
//   - 같은 SVG 는 sha1 파일명으로 캐시한다
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';

const [mdFile, outDir, ...resourceDirs] = process.argv.slice(2);
if (!mdFile || !outDir || resourceDirs.length === 0) {
  console.error('usage: node svg2png.mjs <merged.md> <outDir> <resourceDir...>');
  process.exit(1);
}

const SCALE = 3;          // 인쇄 품질을 위해 3배 해상도로 렌더링
const MAX_WIDTH_IN = 6.5; // template.docx 본문 폭(인치). 이보다 넓으면 본문 폭에 맞춘다

// draw.io 기본 폰트(Helvetica, Tahoma)를 설치된 폰트로 명시 매핑한다.
// Chrome 은 fontconfig 의 metric alias 를 따르지 않아 매핑이 없으면 serif 로 대체된다.
// Helvetica·Tahoma 모두 폭이 비슷한 Liberation Sans(fonts-liberation2)로 매핑해야 라벨이 넘치지 않는다
// (DejaVu Sans 는 폭이 넓어 draw.io 에서 맞춰 둔 줄바꿈이 어긋난다).
const FONT_CSS = `
@font-face { font-family: 'Helvetica'; src: local('Liberation Sans'), local('Arial'); }
@font-face { font-family: 'Helvetica'; font-weight: bold; src: local('Liberation Sans Bold'), local('Arial Bold'); }
@font-face { font-family: 'Tahoma'; src: local('Liberation Sans'), local('Arial'); }
@font-face { font-family: 'Tahoma'; font-weight: bold; src: local('Liberation Sans Bold'), local('Arial Bold'); }
`;

const require = createRequire(import.meta.url);
const puppeteer = require(process.env.PUPPETEER_MODULE || 'puppeteer');

const SVG_IMAGE = /!\[([^\]]*)\]\(<([^>]+\.svg)>\)/g;
const md = fs.readFileSync(mdFile, 'utf8');

const targets = new Map(); // md 안의 경로 -> 실제 파일
for (const [, , src] of md.matchAll(SVG_IMAGE)) {
  if (targets.has(src)) continue;
  const file = resourceDirs.map((d) => path.resolve(d, src)).find((p) => fs.existsSync(p));
  if (file) targets.set(src, file);
  else console.warn(`[svg2png] 파일 없음: ${src}`);
}
if (targets.size === 0) process.exit(0);

fs.mkdirSync(outDir, { recursive: true });
const converted = new Map(); // md 안의 경로 -> { png, width }
// mermaid-cli 와 같은 chrome-headless-shell 을 사용 (별도 Chrome 설치 불필요)
const browser = await puppeteer.launch({ headless: 'shell', args: ['--no-sandbox'] });
try {
  const page = await browser.newPage();
  for (const [src, file] of targets) {
    const hash = crypto.createHash('sha1').update(fs.readFileSync(file)).update(FONT_CSS).digest('hex');
    const png = path.join(outDir, hash + '.png');
    await page.goto(pathToFileURL(file).href);
    await page.evaluate(async (css) => {
      const style = document.createElementNS('http://www.w3.org/2000/svg', 'style');
      style.textContent = css;
      document.documentElement.prepend(style);
      await document.fonts.ready;
    }, FONT_CSS);
    const box = await page.$eval('svg', (el) => {
      const r = el.getBoundingClientRect();
      return { w: r.width, h: r.height };
    });
    if (!fs.existsSync(png)) {
      await page.setViewport({ width: Math.ceil(box.w), height: Math.ceil(box.h), deviceScaleFactor: SCALE });
      await (await page.$('svg')).screenshot({ path: png });
    }
    const widthIn = box.w / 96;
    converted.set(src, { png, width: widthIn > MAX_WIDTH_IN ? '100%' : `${widthIn.toFixed(2)}in` });
  }
} finally {
  await browser.close();
}

fs.writeFileSync(
  mdFile,
  md.replace(SVG_IMAGE, (all, alt, src) => {
    const c = converted.get(src);
    return c ? `![${alt}](<${c.png}>){width="${c.width}"}` : all;
  }),
);
console.log(`[svg2png] SVG ${converted.size}개 -> PNG`);
