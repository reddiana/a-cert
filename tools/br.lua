-- 표 셀 안의 <br> 을 Word 줄바꿈으로 변환
-- (pandoc docx writer 는 raw HTML 을 버리므로 그대로 두면 셀 안의 줄이 한 줄로 붙는다)
function RawInline(el)
  if el.format:match('html') and el.text:match('^<br%s*/?>$') then
    return pandoc.LineBreak()
  end
end
