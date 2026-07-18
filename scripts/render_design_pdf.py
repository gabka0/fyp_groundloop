#!/usr/bin/env python3
"""Render the GroundLoop Markdown design into a readable, dependency-light PDF.

This intentionally supports the small Markdown subset used by the design
document. It uses matplotlib, which is already available in the local host.
"""

from __future__ import annotations

import argparse
import re
import textwrap
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.patches import Rectangle

PAGE_WIDTH = 8.27
PAGE_HEIGHT = 11.69
LEFT = 0.72
RIGHT = 0.68
TOP = 0.72
BOTTOM = 0.66
CONTENT_WIDTH = PAGE_WIDTH - LEFT - RIGHT

INK = "#172033"
MUTED = "#566176"
ACCENT = "#174f70"
ACCENT_2 = "#8c3f2f"
CODE_BG = "#f1f4f6"
RULE = "#cbd3da"


@dataclass(frozen=True)
class Block:
    kind: str
    text: str
    level: int = 0


def clean_inline(text: str) -> str:
    text = re.sub(r"\[([^]]+)]\(([^)]+)\)", r"\1 (\2)", text)
    text = text.replace("**", "").replace("__", "")
    text = text.replace("`", "")
    return text.strip()


def parse_blocks(markdown: str) -> list[Block]:
    lines = markdown.splitlines()
    blocks: list[Block] = []
    paragraph: list[str] = []
    code: list[str] = []
    in_code = False

    def flush_paragraph() -> None:
        if paragraph:
            blocks.append(Block("paragraph", clean_inline(" ".join(paragraph))))
            paragraph.clear()

    for raw in lines:
        line = raw.rstrip()
        if line.startswith("```"):
            flush_paragraph()
            if in_code:
                blocks.append(Block("code", "\n".join(code)))
                code.clear()
            in_code = not in_code
            continue
        if in_code:
            code.append(line)
            continue
        if not line.strip():
            flush_paragraph()
            continue
        heading = re.match(r"^(#{1,4})\s+(.*)$", line)
        if heading:
            flush_paragraph()
            blocks.append(
                Block("heading", clean_inline(heading.group(2)), len(heading.group(1)))
            )
            continue
        if re.match(r"^[-*]\s+", line):
            flush_paragraph()
            blocks.append(Block("bullet", clean_inline(re.sub(r"^[-*]\s+", "", line))))
            continue
        numbered = re.match(r"^(\d+)\.\s+(.*)$", line)
        if numbered:
            flush_paragraph()
            blocks.append(
                Block("number", clean_inline(numbered.group(2)), int(numbered.group(1)))
            )
            continue
        if line.startswith("> "):
            flush_paragraph()
            blocks.append(Block("quote", clean_inline(line[2:])))
            continue
        if line.startswith("|"):
            flush_paragraph()
            if not re.match(r"^\|[\s:|-]+\|$", line):
                cells = [clean_inline(cell) for cell in line.strip("|").split("|")]
                blocks.append(Block("table", "  |  ".join(cells)))
            continue
        paragraph.append(line.strip())

    flush_paragraph()
    if code:
        blocks.append(Block("code", "\n".join(code)))
    return blocks


class Renderer:
    def __init__(self, pdf: PdfPages, source_name: str) -> None:
        self.pdf = pdf
        self.source_name = source_name
        self.fig: plt.Figure | None = None
        self.ax: plt.Axes | None = None
        self.page = 0
        self.y = PAGE_HEIGHT - TOP

    def new_page(self) -> None:
        if self.fig is not None:
            self.finish_page()
        self.fig = plt.figure(figsize=(PAGE_WIDTH, PAGE_HEIGHT), facecolor="white")
        self.ax = self.fig.add_axes((0, 0, 1, 1))
        self.ax.set_xlim(0, PAGE_WIDTH)
        self.ax.set_ylim(0, PAGE_HEIGHT)
        self.ax.axis("off")
        self.page += 1
        self.y = PAGE_HEIGHT - TOP
        if self.page > 1:
            self.ax.text(
                LEFT,
                PAGE_HEIGHT - 0.34,
                "GROUNDLOOP  /  INITIAL TECHNICAL DESIGN",
                fontsize=7.2,
                family="DejaVu Sans",
                color=MUTED,
                va="center",
            )
            self.ax.plot([LEFT, PAGE_WIDTH - RIGHT], [PAGE_HEIGHT - 0.47] * 2, color=RULE, lw=0.55)

    def finish_page(self) -> None:
        assert self.fig is not None and self.ax is not None
        self.ax.plot([LEFT, PAGE_WIDTH - RIGHT], [0.48] * 2, color=RULE, lw=0.45)
        self.ax.text(
            LEFT,
            0.28,
            self.source_name,
            fontsize=6.8,
            family="DejaVu Sans",
            color=MUTED,
            va="center",
        )
        self.ax.text(
            PAGE_WIDTH - RIGHT,
            0.28,
            str(self.page),
            fontsize=7.2,
            family="DejaVu Sans",
            color=MUTED,
            ha="right",
            va="center",
        )
        self.pdf.savefig(self.fig, bbox_inches=None)
        plt.close(self.fig)
        self.fig = None
        self.ax = None

    def require(self, height: float) -> None:
        if self.fig is None:
            self.new_page()
        if self.y - height < BOTTOM:
            self.new_page()

    def title_page(self, title: str) -> None:
        self.new_page()
        assert self.ax is not None
        self.ax.add_patch(
            Rectangle((0, 8.25), PAGE_WIDTH, 3.44, facecolor="#112f42", edgecolor="none")
        )
        self.ax.add_patch(
            Rectangle((0, 7.96), PAGE_WIDTH, 0.29, facecolor="#db7b4d", edgecolor="none")
        )
        wrapped = textwrap.wrap(title, width=30)
        y = 10.77
        for line in wrapped:
            self.ax.text(
                LEFT,
                y,
                line,
                fontsize=25,
                weight="bold",
                family="DejaVu Sans",
                color="white",
                va="top",
            )
            y -= 0.52
        self.ax.text(
            LEFT,
            7.15,
            "VERSIONED NEURAL OBSERVATIONS  /  FACTORIZED IVM  /  RISK-BOUNDED REFRESH",
            fontsize=9.4,
            weight="bold",
            family="DejaVu Sans",
            color=ACCENT,
            va="top",
        )
        self.ax.text(
            LEFT,
            6.52,
            "Initial architecture, formal maintained views, delta rules,\n"
            "consistency protocol, research extensions, and implementation plan",
            fontsize=14,
            family="DejaVu Sans",
            color=INK,
            linespacing=1.45,
            va="top",
        )
        self.ax.text(
            LEFT,
            1.05,
            "GroundLoop FYP  |  Design candidate v0.1  |  17 July 2026",
            fontsize=9,
            family="DejaVu Sans",
            color=MUTED,
            va="bottom",
        )
        self.finish_page()
        self.new_page()

    def draw_wrapped(
        self,
        text: str,
        *,
        x: float,
        width_chars: int,
        fontsize: float,
        line_height: float,
        color: str = INK,
        family: str = "DejaVu Sans",
        weight: str = "normal",
        prefix: str = "",
        hanging: float = 0.0,
    ) -> None:
        lines = textwrap.wrap(
            text,
            width=width_chars,
            break_long_words=False,
            break_on_hyphens=False,
        ) or [""]
        self.require(line_height * len(lines) + 0.04)
        assert self.ax is not None
        for i, line in enumerate(lines):
            line_x = x if i == 0 else x + hanging
            shown = prefix + line if i == 0 else line
            self.ax.text(
                line_x,
                self.y,
                shown,
                fontsize=fontsize,
                family=family,
                weight=weight,
                color=color,
                va="top",
            )
            self.y -= line_height

    def heading(self, text: str, level: int) -> None:
        if level == 2:
            self.require(0.62)
            assert self.ax is not None
            self.y -= 0.12
            self.ax.plot([LEFT, LEFT + 0.42], [self.y + 0.06] * 2, color="#db7b4d", lw=3)
            self.ax.text(
                LEFT + 0.56,
                self.y + 0.12,
                text,
                fontsize=15.2,
                weight="bold",
                family="DejaVu Sans",
                color=ACCENT,
                va="top",
            )
            self.y -= 0.50
        elif level == 3:
            self.require(0.44)
            self.y -= 0.08
            assert self.ax is not None
            self.ax.text(
                LEFT,
                self.y,
                text,
                fontsize=11.2,
                weight="bold",
                family="DejaVu Sans",
                color=ACCENT_2,
                va="top",
            )
            self.y -= 0.35
        else:
            self.draw_wrapped(
                text,
                x=LEFT,
                width_chars=78,
                fontsize=9.4,
                line_height=0.23,
                color=ACCENT,
                weight="bold",
            )
            self.y -= 0.06

    def paragraph(self, text: str) -> None:
        self.draw_wrapped(
            text,
            x=LEFT,
            width_chars=102,
            fontsize=8.65,
            line_height=0.205,
        )
        self.y -= 0.10

    def bullet(self, text: str, number: int | None = None) -> None:
        prefix = f"{number}.  " if number is not None else "•  "
        self.draw_wrapped(
            text,
            x=LEFT + 0.12,
            width_chars=96,
            fontsize=8.55,
            line_height=0.205,
            prefix=prefix,
            hanging=0.25,
        )
        self.y -= 0.055

    def quote(self, text: str) -> None:
        lines = textwrap.wrap(text, width=88, break_long_words=False) or [""]
        height = 0.22 * len(lines) + 0.23
        self.require(height)
        assert self.ax is not None
        self.ax.add_patch(
            Rectangle(
                (LEFT, self.y - height + 0.08),
                CONTENT_WIDTH,
                height,
                facecolor="#edf5f8",
                edgecolor="none",
            )
        )
        self.ax.add_patch(
            Rectangle((LEFT, self.y - height + 0.08), 0.05, height, facecolor=ACCENT, edgecolor="none")
        )
        self.y -= 0.10
        for line in lines:
            self.ax.text(
                LEFT + 0.22,
                self.y,
                line,
                fontsize=9.1,
                style="italic",
                family="DejaVu Serif",
                color=INK,
                va="top",
            )
            self.y -= 0.22
        self.y -= 0.08

    def code(self, text: str) -> None:
        raw_lines: list[str] = []
        for line in text.splitlines() or [""]:
            raw_lines.extend(
                textwrap.wrap(
                    line,
                    width=94,
                    subsequent_indent="  ",
                    replace_whitespace=False,
                    drop_whitespace=False,
                )
                or [""]
            )
        max_lines = 34
        for offset in range(0, len(raw_lines), max_lines):
            lines = raw_lines[offset : offset + max_lines]
            height = 0.183 * len(lines) + 0.22
            self.require(height)
            assert self.ax is not None
            self.ax.add_patch(
                Rectangle(
                    (LEFT, self.y - height + 0.05),
                    CONTENT_WIDTH,
                    height,
                    facecolor=CODE_BG,
                    edgecolor=RULE,
                    linewidth=0.45,
                )
            )
            self.y -= 0.10
            for line in lines:
                self.ax.text(
                    LEFT + 0.16,
                    self.y,
                    line,
                    fontsize=7.35,
                    family="DejaVu Sans Mono",
                    color="#27323a",
                    va="top",
                )
                self.y -= 0.183
            self.y -= 0.10

    def table_row(self, text: str) -> None:
        self.draw_wrapped(
            text,
            x=LEFT + 0.08,
            width_chars=112,
            fontsize=6.85,
            line_height=0.175,
            family="DejaVu Sans Mono",
            color="#26343f",
        )
        assert self.ax is not None
        self.ax.plot([LEFT, PAGE_WIDTH - RIGHT], [self.y + 0.055] * 2, color="#e1e5e8", lw=0.35)
        self.y -= 0.045


def render(blocks: Iterable[Block], output: Path, source_name: str) -> None:
    block_list = list(blocks)
    title = next(
        (block.text for block in block_list if block.kind == "heading" and block.level == 1),
        "GroundLoop Initial Technical Design",
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    with PdfPages(output) as pdf:
        renderer = Renderer(pdf, source_name)
        renderer.title_page(title)
        for block in block_list:
            if block.kind == "heading" and block.level == 1:
                continue
            if block.kind == "heading":
                renderer.heading(block.text, block.level)
            elif block.kind == "paragraph":
                renderer.paragraph(block.text)
            elif block.kind == "bullet":
                renderer.bullet(block.text)
            elif block.kind == "number":
                renderer.bullet(block.text, number=block.level)
            elif block.kind == "quote":
                renderer.quote(block.text)
            elif block.kind == "code":
                renderer.code(block.text)
            elif block.kind == "table":
                renderer.table_row(block.text)
        if renderer.fig is not None:
            renderer.finish_page()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    markdown = args.source.read_text(encoding="utf-8")
    render(parse_blocks(markdown), args.output, args.source.name)


if __name__ == "__main__":
    main()
