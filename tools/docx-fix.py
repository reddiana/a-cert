#!/usr/bin/env python3
"""pandoc 이 만든 docx 를 template.docx 서식과 Word 스키마에 맞게 보정한다 (제자리 수정).

스타일 ID 는 template 마다 달라질 수 있어 모두 스타일 이름으로 찾는다.

1) 표 서식
   template.docx 의 표 서식
     - 표 스타일 'Plain Table 2', 폭 100%, 열 너비 고정
     - 셀 문단 스타일 'Author' + 글자 크기 9pt 직접 서식 ('Author' 스타일 자체는 12pt)
   pandoc 은 모든 표를 'Table' 스타일, 자동 폭, 'Compact' 문단 스타일로 만든다.
   ('Compact' 는 template.docx 에서 목록 번호·들여쓰기가 붙어 있어 셀 안이 목록처럼 보인다)

2) 코드 블록
   template.docx 는 코드 블록을 1칸짜리 'Table Grid' 표로 감싸고 글자를 9pt 로 줄였다.
   pandoc 은 코드 블록을 표 없이 'Source Code' 문단 하나로 만든다 (목록 안이면 목록 번호 속성도 붙는다).

3) 수식(OMML) 스키마 오류
   pandoc 은 \\text{...} 가 든 수식에 <m:nor/> 와 <m:sty/> 를 함께 쓰지만,
   스키마상 둘은 동시에 올 수 없다 (예: $T_{\\text{heal}}$). <m:nor/> 가 있으면 <m:sty/> 를 뺀다.

사용법: python3 tools/docx-fix.py <docx>
"""
import os
import re
import shutil
import sys
import tempfile
import zipfile

TABLE_STYLE_NAME = 'Plain Table 2'
CELL_PARA_STYLE_NAME = 'Author'
PANDOC_CELL_STYLE_NAME = 'Compact'
CODE_TABLE_STYLE_NAME = 'Table Grid'
CODE_PARA_STYLE_NAME = 'Source Code'
# main() 에서 결과 docx 의 styles.xml 로부터 이름 -> 스타일 ID 를 채운다
TABLE_STYLE = CELL_PARA_STYLE = PANDOC_CELL_STYLE = CODE_TABLE_STYLE = CODE_PARA_STYLE = None

SZ = '<w:sz w:val="18"/><w:szCs w:val="18"/>'       # 표 셀 9pt
CODE_SZ = '<w:sz w:val="18"/><w:szCs w:val="20"/>'  # 코드 9pt (template 값 그대로)

# template 코드 표: 들여쓰기 9, 폭 = 본문 폭 + 198 (본문 폭 9360 일 때 template 의 9558)
CODE_TABLE_INDENT = 9
CODE_TABLE_EXTRA_WIDTH = 198

# ECMA-376 스키마상 rPr 안에서 sz/szCs 보다 뒤에 와야 하는 요소
AFTER_SZ = re.compile(r'<w:(highlight|u|effect|bdr|shd|fitText|vertAlign|rtl|cs|em|lang|eastAsianLayout|specVanish|oMath)\b')
# tblPr 안에서 tblLayout 보다 뒤에 와야 하는 요소
AFTER_TBL_LAYOUT = re.compile(r'<w:(tblCellMar|tblLook|tblCaption|tblDescription)\b|</w:tblPr>')


def add_sz(rpr_inner, sz):
    if '<w:sz ' in rpr_inner:
        return rpr_inner
    m = AFTER_SZ.search(rpr_inner)
    return rpr_inner[:m.start()] + sz + rpr_inner[m.start():] if m else rpr_inner + sz


def add_sz_to_runs(xml, sz):
    def fix_run(m):
        run = m.group(0)
        if '<w:rPr>' in run:
            return re.sub(r'<w:rPr>(.*?)</w:rPr>', lambda r: '<w:rPr>' + add_sz(r.group(1), sz) + '</w:rPr>', run, count=1, flags=re.S)
        return run.replace('<w:r>', '<w:r><w:rPr>' + sz + '</w:rPr>', 1)
    return re.sub(r'<w:r>.*?</w:r>', fix_run, xml, flags=re.S)


# ---------------------------------------------------------------- 1) 표 서식

def fix_cell_para(m):
    p = m.group(0)
    if '<w:pPr>' in p:
        def fix_ppr(x):
            # pandoc 기본 문단(Compact)만 바꾸고 table-cells.lua 가 지정한 '표 내부 항목' 등은 유지
            ppr = re.sub(r'<w:pStyle w:val="%s"\s*/>' % re.escape(PANDOC_CELL_STYLE), f'<w:pStyle w:val="{CELL_PARA_STYLE}"/>', x.group(0), count=1)
            if '<w:pStyle ' not in ppr:
                ppr = ppr.replace('<w:pPr>', f'<w:pPr><w:pStyle w:val="{CELL_PARA_STYLE}"/>', 1)
            if '<w:rPr>' not in ppr:
                ppr = ppr.replace('</w:pPr>', '<w:rPr>' + SZ + '</w:rPr></w:pPr>')
            return ppr
        p = re.sub(r'<w:pPr>.*?</w:pPr>', fix_ppr, p, count=1, flags=re.S)
    else:
        p = re.sub(r'^<w:p\b[^>]*>', lambda x: x.group(0) + f'<w:pPr><w:pStyle w:val="{CELL_PARA_STYLE}"/><w:rPr>{SZ}</w:rPr></w:pPr>', p)
    return add_sz_to_runs(p, SZ)


def fix_table(m):
    t = m.group(0)
    t = re.sub(r'<w:tblStyle w:val="[^"]*"\s*/>', f'<w:tblStyle w:val="{TABLE_STYLE}"/>', t, count=1)
    t = re.sub(r'<w:tblW [^>]*/>', '<w:tblW w:w="5000" w:type="pct"/>', t, count=1)
    if '<w:tblLayout ' not in t:
        pos = AFTER_TBL_LAYOUT.search(t).start()
        t = t[:pos] + '<w:tblLayout w:type="fixed"/>' + t[pos:]
    return re.sub(r'<w:p>.*?</w:p>|<w:p [^>]*>.*?</w:p>', fix_cell_para, t, flags=re.S)


# ---------------------------------------------------------------- 2) 코드 블록

def wrap_code_blocks(xml, width):
    pattern = re.compile(r'<w:p><w:pPr><w:pStyle w:val="%s"\s*/>.*?</w:p>' % re.escape(CODE_PARA_STYLE), re.S)
    out, pos, count = [], 0, 0
    for m in pattern.finditer(xml):
        p = m.group(0)
        p = re.sub(r'<w:numPr>.*?</w:numPr>', '', p, count=1, flags=re.S)  # 목록 안 코드 블록의 번호 속성
        p = re.sub(r'<w:pPr>(.*?)</w:pPr>',
                   lambda x: '<w:pPr>' + x.group(1) + ('' if '<w:rPr>' in x.group(1) else f'<w:rPr>{CODE_SZ}</w:rPr>') + '</w:pPr>',
                   p, count=1, flags=re.S)
        p = add_sz_to_runs(p, CODE_SZ)

        table = (
            '<w:tbl><w:tblPr>'
            f'<w:tblStyle w:val="{CODE_TABLE_STYLE}"/><w:tblW w:w="0" w:type="auto"/>'
            f'<w:tblInd w:w="{CODE_TABLE_INDENT}" w:type="dxa"/>'
            '<w:tblLook w:val="04A0" w:firstRow="1" w:lastRow="0" w:firstColumn="1" w:lastColumn="0" w:noHBand="0" w:noVBand="1"/>'
            f'</w:tblPr><w:tblGrid><w:gridCol w:w="{width}"/></w:tblGrid>'
            f'<w:tr><w:tc><w:tcPr><w:tcW w:w="{width}" w:type="dxa"/></w:tcPr>{p}</w:tc></w:tr></w:tbl>'
        )
        # 표가 바로 붙으면 Word 가 하나의 표로 합치므로 빈 문단으로 띄운다
        before = xml[pos:m.start()]
        if (''.join(out) + before).endswith('</w:tbl>'):
            table = '<w:p/>' + table
        if xml.startswith('<w:tbl>', m.end()):
            table = table + '<w:p/>'
        out.append(before)
        out.append(table)
        pos = m.end()
        count += 1
    out.append(xml[pos:])
    return ''.join(out), count


# ---------------------------------------------------------------- 3) 수식

def fix_math_rpr(m):
    inner = m.group(1)
    if re.search(r'<m:nor\b', inner):
        inner = re.sub(r'<m:sty\b[^>]*/>', '', inner)
    return '<m:rPr>' + inner + '</m:rPr>'


# ----------------------------------------------------------------

def style_id(styles_xml, name):
    for m in re.finditer(r'<w:style\b[^>]*>.*?</w:style>', styles_xml, re.S):
        if re.search(r'<w:name w:val="%s"\s*/>' % re.escape(name), m.group(0)):
            return re.search(r'w:styleId="([^"]+)"', m.group(0)).group(1)
    sys.exit(f'[docx-fix] template.docx 에 스타일이 없습니다: {name}')


def text_width(document_xml):
    sect = re.findall(r'<w:sectPr\b.*?</w:sectPr>', document_xml, re.S)[-1]
    page = int(re.search(r'<w:pgSz\b[^>]*w:w="(\d+)"', sect).group(1))
    left = int(re.search(r'<w:pgMar\b[^>]*w:left="(\d+)"', sect).group(1))
    right = int(re.search(r'<w:pgMar\b[^>]*w:right="(\d+)"', sect).group(1))
    return page - left - right


def main(path):
    global TABLE_STYLE, CELL_PARA_STYLE, PANDOC_CELL_STYLE, CODE_TABLE_STYLE, CODE_PARA_STYLE
    with zipfile.ZipFile(path) as src:
        styles = src.read('word/styles.xml').decode('utf-8')
        TABLE_STYLE = style_id(styles, TABLE_STYLE_NAME)
        CELL_PARA_STYLE = style_id(styles, CELL_PARA_STYLE_NAME)
        PANDOC_CELL_STYLE = style_id(styles, PANDOC_CELL_STYLE_NAME)
        CODE_TABLE_STYLE = style_id(styles, CODE_TABLE_STYLE_NAME)
        CODE_PARA_STYLE = style_id(styles, CODE_PARA_STYLE_NAME)

        xml = src.read('word/document.xml').decode('utf-8')
        # 표 보정을 먼저 해야 코드 블록을 감싼 표가 'Plain Table 2' 로 바뀌지 않는다
        xml, tables = re.subn(r'<w:tbl>.*?</w:tbl>', fix_table, xml, flags=re.S)
        xml, codes = wrap_code_blocks(xml, text_width(xml) + CODE_TABLE_EXTRA_WIDTH)
        xml, maths = re.subn(r'<m:rPr>(.*?)</m:rPr>', fix_math_rpr, xml, flags=re.S)

        fd, tmp = tempfile.mkstemp(suffix='.docx', dir=os.path.dirname(os.path.abspath(path)))
        os.close(fd)
        # mkstemp 는 0600 으로 만들어 Samba 로 연 Word 가 열지 못하므로 원본 권한을 유지한다
        shutil.copymode(path, tmp)
        with zipfile.ZipFile(tmp, 'w', zipfile.ZIP_DEFLATED) as dst:
            for item in src.infolist():
                data = xml.encode('utf-8') if item.filename == 'word/document.xml' else src.read(item.filename)
                dst.writestr(item, data)
    os.replace(tmp, path)
    print(f'[docx-fix] 표 {tables}개 template 서식, 코드 블록 {codes}개 표로 감쌈, 수식 속성 {maths}개 점검')


if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.exit('usage: python3 docx-fix.py <docx>')
    main(sys.argv[1])
