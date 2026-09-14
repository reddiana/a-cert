#!/usr/bin/env bash
# ArchitectureSpecification -> Word(docx) 변환
#
# 사용법
#   tools/export-docx.sh [출력.docx]                     기본 출력: ArchitectureSpecification-export.docx
#   tools/export-docx.sh --merged <병합.md> [출력.docx]   Obsidian Advanced Merger 로 병합한 파일을 사용할 때
#   FORCE=1 tools/export-docx.sh ...                     출력 파일이 이미 있으면 덮어쓰기
#
# 단계
#   1) 병합   : tools/merge-spec.mjs (Advanced Merger 플러그인 동작 재현)
#   2) 후처리 : 폴더 헤딩 공백 보정, "Design Approach N - " -> "Design Approach N: "(이미지 임베드 줄 제외),
#               Obsidian 이미지/링크(![[..]], [[..]]) 변환
#   2-1) SVG  : draw.io SVG 를 Chrome 으로 PNG 렌더링(svg2png.mjs)
#   3) pandoc : template.docx 스타일 적용, 표 셀 <br> 줄바꿈(br.lua), mermaid 렌더링(mermaid.lua)
#   4) 보정   : template.docx 표 서식(Plain Table 2, 폭 100%, 셀 9pt), 코드 블록 표(Table Grid, 9pt),
#               수식 스키마 오류(docx-fix.py)
#
# 필요 도구: node, python3, pandoc 3.x, mmdc(@mermaid-js/mermaid-cli, headless Chrome 포함), 한글 폰트
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TOOLS="$ROOT/tools"
BUILD="$ROOT/.export"
cd "$ROOT"

MERGED_INPUT=""
if [[ "${1:-}" == "--merged" ]]; then
  MERGED_INPUT="$(realpath "$2")"
  shift 2
fi
OUTPUT="${1:-ArchitectureSpecification-export.docx}"

# Word 에서 직접 편집한 docx 를 실수로 덮어쓰지 않도록 한다
if [[ -e "$OUTPUT" && "${FORCE:-}" != 1 ]]; then
  echo "이미 존재하는 파일입니다: $OUTPUT (덮어쓰려면 FORCE=1)" >&2
  exit 1
fi

for cmd in node python3 pandoc; do
  command -v "$cmd" >/dev/null || { echo "필요한 도구가 없습니다: $cmd" >&2; exit 1; }
done
MMDC="${MMDC:-$(command -v mmdc || true)}"
[[ -n "$MMDC" ]] || { echo "필요한 도구가 없습니다: mmdc (npm i -g @mermaid-js/mermaid-cli)" >&2; exit 1; }

mkdir -p "$BUILD"
MD="$BUILD/ArchitectureSpecification-merged.md"

# 1) 병합
if [[ -n "$MERGED_INPUT" ]]; then
  cp "$MERGED_INPUT" "$MD"
else
  node "$TOOLS/merge-spec.mjs" ArchitectureSpecification "$MD"
fi

# 2) 후처리
#   - 폴더 헤딩 "#1. Project Overview" -> "# 1. Project Overview" (줄 머리의 헤딩만 보정)
#   - ![[경로]] -> ![](<경로>) : 이미지 임베드 (|크기 옵션은 무시)
#   - [[문서|별칭]] -> 별칭, [[문서]] -> 문서 : Word 에서 깨지는 내부 링크는 텍스트로
sed -i -E '
	s/^(#+)([1-9])/\1 \2/
	/!\[\[/!s/Design Approach ([1-9]) - /Design Approach \1: /g
	s/!\[\[([^]|]+)(\|[^]]*)?\]\]/![](<\1>)/g
	s/\[\[[^]|]+\|([^]]+)\]\]/\1/g
	s/\[\[([^]]+)\]\]/\1/g
' "$MD"

# 2-1) draw.io SVG -> PNG
#   SVG 글자가 HTML(<foreignObject>)이라 Word 에서 사라지므로, mermaid-cli 에 포함된 puppeteer(Chrome)로 렌더링
MMDC_PKG="$(cd "$(dirname "$(readlink -f "$MMDC")")/.." && pwd)"
PUPPETEER_MODULE="$MMDC_PKG/node_modules/puppeteer" \
  node "$TOOLS/svg2png.mjs" "$MD" "$BUILD/svg" . assets assets/drawio assets/sequence assets/mermaid

# 3) pandoc
export MMDC
export MERMAID_OUT_DIR="$BUILD/mermaid"
export MERMAID_CONFIG="$TOOLS/mermaid-config.json"
export MERMAID_PUPPETEER_CFG="$TOOLS/puppeteer-config.json"

pandoc "$MD" -o "$OUTPUT" \
  --reference-doc="$TOOLS/template.docx" \
  -f commonmark_x+hard_line_breaks \
  --lua-filter="$TOOLS/br.lua" \
  --lua-filter="$TOOLS/table-widths.lua" \
  --lua-filter="$TOOLS/table-cells.lua" \
  --lua-filter="$TOOLS/mermaid.lua" \
  --resource-path=".:assets:assets/sequence:assets/drawio:assets/mermaid"

# 4) docx 보정: 표 서식(pandoc 은 표 스타일을 지정할 수 없음), 수식 스키마 오류
python3 "$TOOLS/docx-fix.py" "$OUTPUT"

echo "완료: $OUTPUT"
