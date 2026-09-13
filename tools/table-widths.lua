-- 표 열 너비를 셀 내용 길이 비율로 지정
--
-- commonmark_x 입력은 파이프 표 구분선의 길이 정보를 버려 모든 열이 같은 폭이 된다.
-- (기존 markdown 입력은 구분선 '-' 개수 비율을 썼고, 표 편집 플러그인이 구분선을 내용 길이에 맞춰 두므로 결과가 비슷하다)
--   - 비율: 열마다 본문 셀의 가장 긴 줄 표시 폭 (<br> 줄바꿈은 줄 단위, 한글 등 전각 문자는 2칸)
--           머리행은 줄바꿈되어도 되므로 가장 긴 단어만 반영
--   - 최소 폭: 열에서 가장 긴 단어가 끊기지 않을 폭 (본문 폭에 9pt 로 들어가는 반각 문자 수 기준)
-- br.lua 다음에 실행해야 <br> 이 줄바꿈으로 계산된다.

local CHARS_PER_LINE = 80  -- template.docx 본문 폭 6.5in 에 9pt(굵은 머리글 포함)로 들어가는 반각 문자 수(보수적 근사)
local CELL_PADDING = 3     -- 셀 좌우 여백·굵은 머리글 보정(반각 문자 수)
local MIN_RATIO = 0.06

local function is_wide(cp)
  return (cp >= 0x1100 and cp <= 0x11FF)   -- 한글 자모
      or (cp >= 0x2E80 and cp <= 0xA4CF)   -- CJK 부호·한자·【】 등
      or (cp >= 0xAC00 and cp <= 0xD7A3)   -- 한글 음절
      or (cp >= 0xF900 and cp <= 0xFAFF)   -- CJK 호환 한자
      or (cp >= 0xFF00 and cp <= 0xFF60)   -- 전각 영숫자
end

local function display_len(s)
  local n = 0
  for _, cp in utf8.codes(s) do
    n = n + (is_wide(cp) and 2 or 1)
  end
  return n
end

local MIN_WORD = 4 -- 한글 두 음절 폭

-- 줄바꿈으로 끊을 수 없는 가장 긴 덩어리의 표시 폭
-- (한글·한자는 음절 단위로 줄바꿈되므로 영문·숫자·기호 연속 구간만 센다: BatchService, QAS-01, (30%))
local function longest_unbreakable(line)
  local max, run = 0, 0
  for _, cp in utf8.codes(line) do
    if cp == 0x20 or is_wide(cp) then
      run = 0
    else
      run = run + 1
      max = math.max(max, run)
    end
  end
  return math.max(max, MIN_WORD)
end

-- 셀의 가장 긴 줄과 가장 긴 끊을 수 없는 덩어리의 표시 폭
local function measure_cell(cell)
  local blocks = pandoc.Blocks(cell.contents):walk {
    LineBreak = function() return pandoc.Str('\n') end,
  }
  local line_max, word_max = 0, 0
  for line in (pandoc.utils.stringify(blocks) .. '\n'):gmatch('([^\n]*)\n') do
    line_max = math.max(line_max, display_len(line))
    word_max = math.max(word_max, longest_unbreakable(line))
  end
  return line_max, word_max
end

-- 최소 폭을 보장하면서 나머지 폭을 내용 비율대로 나눈다
local function allocate(content, minimum)
  local n = #content
  local ratios, fixed = {}, {}
  for _ = 1, n do
    local free_total, fixed_sum = 0, 0
    for i = 1, n do
      if fixed[i] then fixed_sum = fixed_sum + minimum[i] else free_total = free_total + content[i] end
    end
    local changed = false
    for i = 1, n do
      if not fixed[i] then
        ratios[i] = (1 - fixed_sum) * content[i] / free_total
        if ratios[i] < minimum[i] then
          fixed[i], changed = true, true
        end
      else
        ratios[i] = minimum[i]
      end
    end
    if not changed then break end
  end
  return ratios
end

function Table(tbl)
  for _, spec in ipairs(tbl.colspecs) do
    if spec[2] ~= nil then return nil end -- 너비가 이미 지정된 표는 그대로 둔다
  end

  local ncol = #tbl.colspecs
  local content, words = {}, {}
  for i = 1, ncol do content[i], words[i] = 1, 1 end

  local function measure(rows, is_head)
    for _, row in ipairs(rows) do
      local col = 1
      for _, cell in ipairs(row.cells) do
        if cell.col_span == 1 and col <= ncol then
          local line_max, word_max = measure_cell(cell)
          content[col] = math.max(content[col], is_head and word_max or line_max)
          words[col] = math.max(words[col], word_max)
        end
        col = col + cell.col_span
      end
    end
  end
  measure(tbl.head.rows, true)
  for _, body in ipairs(tbl.bodies) do
    measure(body.head, true)
    measure(body.body, false)
  end

  local minimum, min_sum = {}, 0
  for i = 1, ncol do
    minimum[i] = math.max(MIN_RATIO, (words[i] + CELL_PADDING) / CHARS_PER_LINE)
    min_sum = min_sum + minimum[i]
  end
  if min_sum > 1 then -- 열이 너무 많으면 최소 폭끼리 비율 조정
    for i = 1, ncol do minimum[i] = minimum[i] / min_sum end
  end

  local ratios = allocate(content, minimum)
  for i, spec in ipairs(tbl.colspecs) do
    tbl.colspecs[i] = { spec[1], ratios[i] }
  end
  return tbl
end
