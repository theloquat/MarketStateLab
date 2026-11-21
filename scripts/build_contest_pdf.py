"""Build the product/technical acceptance PDF from versioned release documents.

Install reportlab; set QUANTOS_CJK_FONT to an embeddable CJK TrueType font on Linux.
The generated document states measured performance limits rather than hiding them.
"""

from pathlib import Path
import html
import json
import math
import os
import re
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    PageBreak,
    Table,
    TableStyle,
    Image,
)
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.colors import HexColor
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.graphics.shapes import Drawing, Rect, String, Line, Polygon

ROOT = Path(__file__).resolve().parents[1]
font = os.getenv("QUANTOS_CJK_FONT", "/System/Library/Fonts/STHeiti Light.ttc")
if not Path(font).is_file():
    raise SystemExit("Set QUANTOS_CJK_FONT to an embeddable CJK TrueType font.")
pdfmetrics.registerFont(TTFont("CJK", font, subfontIndex=0))
NAVY, MUTED, ACCENT = [HexColor(x) for x in ["#23354e", "#62748b", "#5c68b5"]]
styles = {
    k: ParagraphStyle(
        k,
        fontName="CJK",
        fontSize=size,
        leading=leading,
        textColor=NAVY,
        spaceAfter=gap,
        wordWrap="CJK",
    )
    for k, size, leading, gap in [
        ("body", 10.5, 17, 10),
        ("h1", 24, 34, 22),
        ("h2", 15, 24, 14),
        ("small", 8.5, 13, 6),
    ]
}
styles["h1"].keepWithNext = True
styles["h2"].keepWithNext = True
story = []


def normalized(text):
    return text.translate({ord(c): "-" for c in "‐‑‒–—―−"})


def para(text, kind="body"):
    text = normalized(text)
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text).replace("`", "")
    story.append(Paragraph(html.escape(text), styles[kind]))


def box(d, x, y, title, subtitle="", width=105, height=54):
    d.add(
        Rect(
            x,
            y,
            width,
            height,
            rx=7,
            fillColor=HexColor("#f0f3fa"),
            strokeColor=HexColor("#d7dfef"),
        )
    )
    d.add(String(x + 8, y + 32, title, fontName="CJK", fontSize=10, fillColor=NAVY))
    if subtitle:
        d.add(
            String(x + 8, y + 13, subtitle, fontName="CJK", fontSize=8, fillColor=MUTED)
        )


def edge(d, points, label="", position=None):
    for (x1, y1), (x2, y2) in zip(points, points[1:]):
        d.add(Line(x1, y1, x2, y2, strokeColor=ACCENT, strokeWidth=0.8))
    (x1, y1), (x2, y2) = points[-2:]
    angle = math.atan2(y2 - y1, x2 - x1)
    dx, dy = math.cos(angle), math.sin(angle)
    d.add(
        Polygon(
            [
                x2,
                y2,
                x2 - 6 * dx + 2.5 * dy,
                y2 - 6 * dy - 2.5 * dx,
                x2 - 6 * dx - 2.5 * dy,
                y2 - 6 * dy + 2.5 * dx,
            ],
            fillColor=ACCENT,
            strokeColor=ACCENT,
        )
    )
    if label and position:
        d.add(String(*position, label, fontName="CJK", fontSize=8, fillColor=MUTED))


def architecture():
    d = Drawing(480, 420)
    rows = [
        (
            340,
            [
                ("官方ZIP / PDF", "原始资料"),
                ("数据 API / UDM", "校验、单位、血缘"),
                ("MinIO 原件", "模型 / Savepoint"),
                ("SQLite 研究目录", "离线参照与材料"),
            ],
        ),
        (
            230,
            [
                ("独立发布器", "断点 / 可选配速"),
                ("Kafka 观测", "持久事件流"),
                ("Flink 特征+评分", "日窗口 / 冻结模型"),
                ("Kafka 特征/状态", "稳定事件标识"),
            ],
        ),
        (
            120,
            [
                ("Redis 热状态", "已提交事件广播"),
                ("Kafka 持久投递", "outbox 重试"),
                ("MySQL 事务", "状态 / 版本 / outbox"),
                ("特征 / 预警服务", "微批 / 冷却 / 权限"),
            ],
        ),
        (
            10,
            [
                ("Java Gateway", "JWT / Caffeine / WS"),
                ("React 工作台", "查询与研究闭环"),
                ("独立物化线程", "持久批信封"),
                ("ClickHouse", "月分区分析事实"),
            ],
        ),
    ]
    for y, nodes in rows:
        for i, (title, sub) in enumerate(nodes):
            box(d, 125 * i, y, title, sub)
    for i in range(2):
        edge(d, [(125 * i + 105, 367), (125 * (i + 1), 367)])
    edge(d, [(177, 394), (177, 410), (427, 410), (427, 394)])
    for i in range(3):
        edge(d, [(125 * i + 105, 257), (125 * (i + 1), 257)])
    edge(d, [(177, 340), (177, 307), (52, 307), (52, 284)])
    edge(d, [(302, 340), (302, 284)])
    edge(d, [(427, 230), (427, 174)])
    for i in [3, 2, 1]:
        edge(d, [(125 * i, 147), (125 * (i - 1) + 105, 147)])
    edge(d, [(52, 120), (52, 64)])
    edge(d, [(105, 37), (125, 37)])
    edge(d, [(302, 120), (302, 64)])
    edge(d, [(355, 37), (375, 37)])
    return d


def overview():
    d = Drawing(480, 210)
    labels = [
        ("多源原件 / UDM", "来源与单位"),
        ("Kafka / Flink", "特征与冻结推断"),
        ("状态 / 预警", "可追溯研究"),
        ("MinIO / 原始证据", "原件和检查点"),
        ("MySQL / CH / Redis", "主事务、分析与缓存"),
        ("Java网关 / React", "权限与用户工作台"),
    ]
    for i, (title, subtitle) in enumerate(labels):
        x, y = (i % 3) * 167, 135 if i < 3 else 35
        box(d, x, y, title, subtitle, width=146)
    for x in [146, 313]:
        edge(d, [(x, 162), (x + 21, 162)])
    for x in [73, 240, 407]:
        edge(d, [(x, 135), (x, 89)])
    return d


def entities():
    d = Drawing(480, 445)
    for x, y, title, sub in [
        (0, 350, "Dataset 数据集", "PK: id / 原包哈希"),
        (125, 350, "Series 资产", "PK: dataset + id"),
        (250, 350, "Document 文档", "PK: id / 来源"),
        (375, 350, "PDFPage 原页", "PK: document + 页码"),
        (0, 240, "Model 冻结模型", "PK: id / 参数哈希"),
        (125, 240, "Run 回放", "PK: id / 冻结配置"),
        (250, 240, "MarketState 状态", "PK: run + day"),
        (375, 240, "Alert 预警", "PK: 稳定告警id"),
        (0, 130, "FilterState 递推", "PK: run + day"),
        (125, 130, "DayFeature 特征", "PK: run + day"),
        (250, 130, "Outbox 投递", "PK: 稳定事件id"),
        (0, 20, "Research 研究", "PK: id / 输入版本"),
        (125, 20, "Audit 审批", "身份 / 动作 / 时间"),
        (250, 20, "AgentCall 调用", "PK: id / 预算 / 使用量"),
    ]:
        box(d, x, y, title, sub)
    edge(d, [(105, 377), (125, 377)])
    edge(d, [(52, 404), (52, 425), (302, 425), (302, 404)], "1:N", (168, 430))
    edge(d, [(355, 377), (375, 377)])
    edge(d, [(52, 350), (52, 294)], "1:N", (58, 320))
    for i in range(3):
        edge(d, [(i * 125 + 105, 267), ((i + 1) * 125, 267)])
    edge(d, [(177, 240), (177, 184)], "1:N", (183, 210))
    edge(d, [(155, 240), (155, 212), (52, 212), (52, 184)])
    edge(d, [(200, 240), (200, 212), (302, 212), (302, 184)])
    edge(d, [(105, 47), (125, 47)])
    edge(d, [(302, 74), (302, 101), (52, 101), (52, 74)])
    return d


para("Market State Lab", "h1")
para("金融市场状态监控与智能投研平台", "h1")
para("产品与技术说明 / 验收候选 / 2026-11-21", "h2")
para(
    "三项赛事任务由同一条真实数据链路承接。功能、运行正确性与性能分别给出证据；未达成或未验证的指标如实列明，不宣称全部达标或保证获奖。"
)
story.append(Spacer(1, 12))
story.append(overview())
para(
    "金融市场状态、预警与研究证据可追溯到输入数据和冻结模型版本。",
    "small",
)
story.append(PageBreak())
para("实际事件与事务链路", "h1")
para(
    "Flink执行日窗口特征与冻结推断；旧运行仍可走Python推断。服务端先提交MySQL，再通过持久投递更新Kafka/Redis和分析副本。"
)
story.append(architecture())
para(
    "REST/GraphQL查询由Java网关路由至数据、特征与预警API；WS通过事件广播推送。图中突出主数据流，查询与审批接口未逐条连线。",
    "small",
)
story.append(PageBreak())
para("数据关系与存储职责", "h1")
story.append(entities())
para(
    "箭头表示领域的一对多或关联关系。MySQL保护分布式运行与投递事务，SQLite管理本地资料/离线研究目录，原件/模型/检查点在MinIO，分析副本在ClickHouse；不宣称存在跨数据库外键或跨库严格exactly-once。"
)
para(
    "ODS保留原件；DWD保存规范观察及来源；DWS保存版本化特征；ADS提供状态、预警与研究证据。传输字段顺序的优化不改变规范内容哈希。"
)

shot = ROOT / "docs/contest/standalone-replay.png"
if shot.exists():
    story.append(PageBreak())
    para("产品工作台与证据", "h1")
    para("历史回放自动订阅已提交的状态，展示冻结版本、概率、规则对照、预警和按时间排列的状态网格。")
    picture = Image(str(shot))
    picture.drawHeight = picture.imageHeight * 480 / picture.imageWidth
    picture.drawWidth = 480
    story.append(picture)
    para("高压力概率表示模型的状态分类结果。", "small")

files = [
    "架构与接口.md",
    "数据与模型卡.md",
    "使用与验收.md",
    "性能与测试报告.md",
    "AI协作与技术来源.md",
    "视频脚本与验收清单.md",
    "ACCEPTANCE.md",
]
for name in files:
    story.append(PageBreak())
    code, table = False, []

    def flush():
        if not table:
            return
        rows = [
            [
                Paragraph(html.escape(normalized(v.strip())), styles["small"])
                for v in row
            ]
            for row in table
        ]
        widths = [480 / len(rows[0])] * len(rows[0])
        t = Table(rows, colWidths=widths, repeatRows=1, hAlign="LEFT")
        t.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), HexColor("#edf1f9")),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("LINEBELOW", (0, 0), (-1, 0), 0.7, ACCENT),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                    ("TOPPADDING", (0, 0), (-1, -1), 8),
                ]
            )
        )
        story.extend([t, Spacer(1, 12)])
        table.clear()

    for line in (ROOT / "docs/release" / name).read_text().splitlines():
        if line.startswith("|"):
            if not re.match(r"^\|[\s:|\-]+$", line):
                table.append(line.strip("|").split("|"))
            continue
        flush()
        if line.startswith("```"):
            code = not code
            continue
        if not line.strip():
            continue
        if line.startswith("# "):
            para(line[2:], "h1")
        elif line.startswith("## "):
            para(line[3:], "h2")
        else:
            para(line, "small" if code else "body")
    flush()

summary = json.loads((ROOT / "docs/contest/standalone-test-summary.json").read_text())
story.append(PageBreak())
para("测试与证据索引", "h1")
para("冻结代码测试", "h2")
p = summary["python"]
para(
    f"Python {p['tests']}项通过，Python 业务包行覆盖{p['percent']:.2f}%。前端{summary['frontend']['tests']}项交互通过，行覆盖{summary['frontend']['coverage']['lines']['pct']:.2f}%。"
)
for key, label in [("flink", "Flink"), ("gateway", "Java网关")]:
    v = summary["java"][key]
    para(
        f"{label}：{v['tests']}项Java测试通过，JaCoCo行覆盖{v['counters']['LINE']['percent']:.2f}%。真实容器集成和故障恢复另外验证，不冒充单元覆盖。"
    )
para("证据如何复核", "h2")
para(
    "docs/contest保留原始测试、时间评价、研究、费用、干净启动、故障与压力报告；docs/adr记录决策与代价；docs/release提供接口、数据模型、操作、性能、视频脚本与答辩阅读路线。界面“运行监控”可下载公开JSON证据。"
)
para(
    "不在此文件重印官方数据或赛题。原始包由获授权使用者提供，源码发布包排除数据、真实密钥、本机凭证和临时测试卷。"
)
para("LLM费用与录制", "h2")
para(
    "真实联调合计2次、1600tokens；失败也占预算，未自动重试。录制可以展示已保存规划与工具结果，无需再次花费API额度。此处交付系统与材料，实际录制、上传和提交等待用户验收。"
)

out = ROOT / "output/pdf/MarketStateLab-产品及技术方案.pdf"
out.parent.mkdir(parents=True, exist_ok=True)
page_count = 0


def page(c, doc):
    global page_count
    page_count = doc.page
    c.setStrokeColor(HexColor("#dce2ee"))
    c.line(48, 44, 547, 44)
    c.setFillColor(MUTED)
    c.setFont("CJK", 8)
    c.drawString(48, 30, "Market State Lab / 性能边界见实测报告")
    c.drawRightString(547, 30, str(doc.page))


SimpleDocTemplate(
    str(out),
    pagesize=(595.28, 841.89),
    leftMargin=54,
    rightMargin=54,
    topMargin=52,
    bottomMargin=64,
    title="市场状态监控平台 产品与技术说明",
    author="Market State Lab",
).build(story, onFirstPage=page, onLaterPages=page)
if page_count > 40:
    raise RuntimeError("PDF exceeds 40-page submission limit")
print(str(out), page_count, "pages")
