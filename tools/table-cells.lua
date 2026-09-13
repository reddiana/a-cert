-- 표 셀 안의 "- 항목<br>- 항목" 줄 목록을 template.docx 의 '표 내부 항목' 글머리표 문단으로 변환
--
-- template.docx 는 셀 안 목록을 '표 내부 항목' 스타일(글머리표, 왼쪽 정렬, 내어쓰기, 9pt) 문단으로 정리했다.
-- 마크다운 표는 셀 안에 목록을 쓸 수 없어 "- " 줄을 <br> 로 이어 쓰므로, 이를 문단으로 나눈다.
--   - "- ", "* ", "• " 로 시작하는 줄이 MIN_BULLETS 개 이상인 문단만 변환
--   - 글머리표가 없는 줄은 일반 문단으로 남긴다
-- br.lua 다음에 실행해야 한다 (<br> 이 줄바꿈으로 바뀐 뒤).

local BULLET_STYLE = '표 내부 항목'
local MIN_BULLETS = 2
local BULLET_MARKS = { ['-'] = true, ['*'] = true, ['•'] = true }

local function split_lines(inlines)
  local lines, cur = {}, pandoc.Inlines {}
  for _, el in ipairs(inlines) do
    if el.t == 'LineBreak' then
      table.insert(lines, cur)
      cur = pandoc.Inlines {}
    else
      cur:insert(el)
    end
  end
  table.insert(lines, cur)
  return lines
end

-- 줄 머리의 글머리표를 떼어 내고, 글머리표 줄이었는지 돌려준다
local function strip_bullet(line)
  while #line > 0 and (line[1].t == 'Space' or line[1].t == 'SoftBreak') do
    line:remove(1)
  end
  if #line >= 2 and line[1].t == 'Str' and BULLET_MARKS[line[1].text] and line[2].t == 'Space' then
    line:remove(1)
    line:remove(1)
    return line, true
  end
  return line, false
end

local function convert(blocks)
  local out, changed = pandoc.Blocks {}, false
  for _, block in ipairs(blocks) do
    local lines = (block.t == 'Plain' or block.t == 'Para') and split_lines(block.content) or nil
    local parsed, bullets = {}, 0
    for i, line in ipairs(lines or {}) do
      local content, is_bullet = strip_bullet(line)
      parsed[i] = { content = content, bullet = is_bullet }
      if is_bullet then bullets = bullets + 1 end
    end

    if bullets >= MIN_BULLETS then
      changed = true
      for _, p in ipairs(parsed) do
        if p.bullet then
          out:insert(pandoc.Div({ pandoc.Para(p.content) }, { ['custom-style'] = BULLET_STYLE }))
        elseif #p.content > 0 then
          out:insert(pandoc.Plain(p.content))
        end
      end
    else
      out:insert(block)
    end
  end
  return changed and out or nil
end

function Table(tbl)
  local function each(rows)
    for _, row in ipairs(rows) do
      for _, cell in ipairs(row.cells) do
        local blocks = convert(cell.contents)
        if blocks then cell.contents = blocks end
      end
    end
  end
  each(tbl.head.rows)
  for _, body in ipairs(tbl.bodies) do
    each(body.head)
    each(body.body)
  end
  each(tbl.foot.rows)
  return tbl
end
