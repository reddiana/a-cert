-- ```mermaid 코드 블록을 mermaid-cli(mmdc)로 PNG 렌더링해 이미지로 치환
--
-- 환경 변수 (tools/export-docx.sh 가 설정)
--   MMDC                  mmdc 실행 파일            (기본: mmdc)
--   MERMAID_OUT_DIR       렌더링 결과 저장 폴더      (기본: .export/mermaid)
--   MERMAID_CONFIG        mermaid 설정 JSON (폰트 등)
--   MERMAID_PUPPETEER_CFG puppeteer 설정 JSON (--no-sandbox 등)
--
-- 같은 내용의 다이어그램은 sha1 파일명으로 캐시하여 다시 렌더링하지 않는다.
-- 렌더링에 실패하면 경고만 남기고 코드 블록을 그대로 둔다.

local MMDC = os.getenv('MMDC') or 'mmdc'
local OUT_DIR = os.getenv('MERMAID_OUT_DIR') or '.export/mermaid'
local CONFIG = os.getenv('MERMAID_CONFIG')
local PUPPETEER_CFG = os.getenv('MERMAID_PUPPETEER_CFG')

local SCALE = 3          -- 인쇄 품질을 위해 3배 해상도로 렌더링
local MAX_WIDTH_IN = 6.5 -- template.docx 본문 폭(인치). 이보다 넓으면 본문 폭에 맞춘다

local function file_exists(p)
  local f = io.open(p, 'rb')
  if f then f:close() return true end
  return false
end

local function png_width_px(p)
  local f = assert(io.open(p, 'rb'))
  local header = f:read(24)
  f:close()
  return string.unpack('>I4', header, 17) -- IHDR width
end

function CodeBlock(el)
  if not el.classes:includes('mermaid') then return nil end

  -- gantt 의 오늘 날짜 표시선은 변환 시점마다 달라지므로 문서에서는 끈다
  local text = el.text
  if text:match('^%s*gantt') and not text:match('todayMarker') then
    text = text:gsub('^(%s*gantt[^\n]*)', '%1\n    todayMarker off', 1)
  end

  pandoc.system.make_directory(OUT_DIR, true)
  local base = OUT_DIR .. '/' .. pandoc.utils.sha1(text)
  local png = base .. '.png'

  if not file_exists(png) then
    local src = assert(io.open(base .. '.mmd', 'w'))
    src:write(text)
    src:close()

    local args = { '-i', base .. '.mmd', '-o', png, '-s', tostring(SCALE), '-b', 'white' }
    if CONFIG then table.insert(args, '-c') table.insert(args, CONFIG) end
    if PUPPETEER_CFG then table.insert(args, '-p') table.insert(args, PUPPETEER_CFG) end

    local ok, err = pcall(pandoc.pipe, MMDC, args, '')
    if not ok or not file_exists(png) then
      pandoc.log.warn('mermaid 렌더링 실패, 코드 블록 유지: ' .. tostring(err))
      return nil
    end
  end

  -- 3배 해상도 PNG 를 원래 크기(96dpi 기준)로 배치, 본문 폭 초과 시 100%
  local width_in = png_width_px(png) / SCALE / 96
  local width = width_in > MAX_WIDTH_IN and '100%' or string.format('%.2fin', width_in)

  return pandoc.Para { pandoc.Image({}, png, '', pandoc.Attr('', {}, { width = width })) }
end
