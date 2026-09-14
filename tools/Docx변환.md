## 자동 변환 (권장)
병합 → 후처리 → pandoc 을 한 번에 수행한다. 결과는 기본적으로 `ArchitectureSpecification-export.docx` 이며, 같은 이름의 파일이 있으면 덮어쓰지 않고 중단한다 (`FORCE=1` 로 덮어쓰기).
```bash
tools/export-docx.sh                      # ArchitectureSpecification 폴더를 직접 병합
tools/export-docx.sh 결과.docx            # 출력 파일 지정
tools/export-docx.sh --merged ArchitectureSpecification-merged.md   # Obsidian Advanced Merger 결과 사용
```
- 병합: `tools/merge-spec.mjs` 가 Advanced Merger 플러그인(logical 정렬, 폴더/파일명 헤딩)과 같은 결과를 만든다.
- 중간 산출물(병합 md, mermaid PNG 캐시)은 `.export/` 에 생성된다 (git 제외).

## 수동 절차 (Obsidian 으로 병합한 경우)
### 후처리
```bash
sed -i -E '
	s/^(#+)([1-9])/\1 \2/
	s/Design Approach ([1-9]) - /Design Approach \1: /g
	s/!\[\[([^]|]+)(\|[^]]*)?\]\]/![](<\1>)/g
	s/\[\[[^]|]+\|([^]]+)\]\]/\1/g
	s/\[\[([^]]+)\]\]/\1/g
' ArchitectureSpecification-merged.md
```
- `^(#+)([1-9])`: 폴더 헤딩 `#1. Project Overview` → `# 1. Project Overview` (본문의 `Replica #1` 등은 건드리지 않음)
- `![[경로]]` → `![](<경로>)`: Obsidian 이미지 임베드를 표준 마크다운으로
- `[[문서]]`, `[[문서|별칭]]` → 텍스트: Word 에서 깨지는 내부 링크 제거

### draw.io SVG → PNG
draw.io SVG 는 글자를 HTML(`<foreignObject>`)로 담고 있어 Word 에 SVG 로 넣으면 글자가 모두 사라진다. headless Chrome 으로 PNG 렌더링하고 md 의 참조를 바꾼다.
```bash
PUPPETEER_MODULE=~/.local/lib/node_modules/@mermaid-js/mermaid-cli/node_modules/puppeteer \
  node tools/svg2png.mjs ArchitectureSpecification-merged.md "$PWD/.export/svg" . assets
```

### pandoc
```bash
pandoc ArchitectureSpecification-merged.md -o ArchitectureSpecification-merged.docx \
  --reference-doc=template.docx \
  -f commonmark_x+hard_line_breaks \
  --lua-filter=tools/br.lua \
  --lua-filter=tools/table-widths.lua \
  --lua-filter=tools/table-cells.lua \
  --lua-filter=tools/mermaid.lua \
  --resource-path=.:assets
```
- `table-widths.lua`: `commonmark_x` 는 표 구분선 길이를 버려 모든 열이 같은 폭이 되므로, 셀 내용 길이 비율로 열 너비 지정 (br.lua 다음)
- `table-cells.lua`: 셀 안의 `- 항목<br>- 항목` 을 template 의 `표 내부 항목` 글머리표 문단으로 분리 (br.lua 다음)
- `commonmark_x`: 빈 줄 없이 이어지는 `#` 제목, `-` 목록을 Obsidian 과 같이 인식
- `hard_line_breaks`: 줄바꿈 한 번을 Word 줄바꿈으로 유지
- `br.lua`: 표 셀 안의 `<br>` 을 줄바꿈으로 (없으면 셀 내용이 한 줄로 붙음)
- `mermaid.lua`: ` ```mermaid ` 블록을 PNG 로 렌더링해 삽입 (mermaid-cli 필요, 수동 실행 시 `MERMAID_PUPPETEER_CFG=tools/puppeteer-config.json MERMAID_CONFIG=tools/mermaid-config.json` 지정 권장)

### docx 보정 (표 서식, 코드 블록, 수식 스키마)
- 표: pandoc 은 모든 표를 `Table` 스타일·자동 폭·`Compact` 문단으로 만든다. template.docx 의 표 서식(`Plain Table 2`, 폭 100%, 열 너비 고정, 셀 문단 `Author` + 9pt)으로 보정한다.
- 코드 블록: pandoc 은 표 없는 `Source Code` 문단으로 만든다. template.docx 처럼 1칸 `Table Grid` 표로 감싸고 9pt 로 줄인다 (목록 안 코드 블록의 번호 속성 제거).
- 수식: `$T_{\text{heal}}$` 처럼 `\text{}` 가 든 수식은 pandoc 이 스키마에 어긋나는 OMML(`m:nor` + `m:sty`)을 만들므로 `m:sty` 를 제거한다.
```bash
python3 tools/docx-fix.py ArchitectureSpecification-merged.docx
```

## 필요 도구 설치 (Ubuntu)
```bash
# fonts-liberation2: draw.io 기본 폰트 Helvetica 와 글자 폭이 같은 대체 폰트 (없으면 라벨이 넘쳐 잘림)
sudo apt-get install -y pandoc fonts-nanum fonts-noto-cjk fonts-liberation2
npm i -g --prefix ~/.local @mermaid-js/mermaid-cli
# headless Chrome 실행 라이브러리
sudo apt-get install -y libatk1.0-0t64 libatk-bridge2.0-0t64 libxdamage1 libasound2t64 libatspi2.0-0t64
```
- mermaid-cli 설치 시 Chrome 다운로드가 실패하면 puppeteer 가 요구하는 버전의 `chrome-headless-shell-linux64.zip` 을 `https://storage.googleapis.com/chrome-for-testing-public/<버전>/linux64/` 에서 받아 `~/.cache/puppeteer/chrome-headless-shell/linux-<버전>/` 에 압축 해제한다.
