# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class GithubTemplateRepo:
    url: str


@dataclass
class TemplateOption:
    id: str
    name: str
    description: str
    source: GithubTemplateRepo
    llama_cloud: bool


UI_TEMPLATES = [
    TemplateOption(
        id="basic-ui",
        name="Basic UI",
        description="Starter workflow with React Vite UI",
        source=GithubTemplateRepo(
            url="https://github.com/run-llama/template-workflow-basic-ui"
        ),
        llama_cloud=False,
    ),
    TemplateOption(
        id="showcase",
        name="Showcase",
        description="Workflow and UI pattern examples for LlamaAgents apps",
        source=GithubTemplateRepo(
            url="https://github.com/run-llama/template-workflow-showcase"
        ),
        llama_cloud=False,
    ),
    TemplateOption(
        id="document-qa",
        name="Document Question & Answer",
        description="Document upload and Q&A with React UI",
        source=GithubTemplateRepo(
            url="https://github.com/run-llama/template-workflow-document-qa"
        ),
        llama_cloud=True,
    ),
    TemplateOption(
        id="extraction-review",
        name="Extraction Agent with Review UI",
        description="Schema-based document extraction with review UI (LlamaExtract)",
        source=GithubTemplateRepo(
            url="https://github.com/run-llama/template-workflow-data-extraction"
        ),
        llama_cloud=True,
    ),
    TemplateOption(
        id="classify-extract-sec",
        name="SEC Insights",
        description="SEC filing classification and key info extraction",
        source=GithubTemplateRepo(
            url="https://github.com/run-llama/template-workflow-classify-extract-sec"
        ),
        llama_cloud=True,
    ),
    TemplateOption(
        id="extract-reconcile-invoice",
        name="Invoice Extraction & Reconciliation",
        description="Invoice extraction and reconciliation against contracts",
        source=GithubTemplateRepo(
            url="https://github.com/run-llama/template-workflow-extract-reconcile-invoice"
        ),
        llama_cloud=True,
    ),
]

HEADLESS_TEMPLATES = [
    TemplateOption(
        id="basic",
        name="Basic Workflow",
        description="Starter workflow usage patterns (API only, no UI)",
        source=GithubTemplateRepo(
            url="https://github.com/run-llama/template-workflow-basic"
        ),
        llama_cloud=False,
    ),
    TemplateOption(
        id="document_parsing",
        name="Document Parser",
        description="Parse unstructured documents to text via LlamaParse",
        source=GithubTemplateRepo(
            url="https://github.com/run-llama/template-workflow-document-parsing"
        ),
        llama_cloud=True,
    ),
    TemplateOption(
        id="human_in_the_loop",
        name="Human in the Loop",
        description="Human-in-the-loop workflow pattern",
        source=GithubTemplateRepo(
            url="https://github.com/run-llama/template-workflow-human-in-the-loop"
        ),
        llama_cloud=False,
    ),
    TemplateOption(
        id="invoice_extraction",
        name="Invoice Extraction",
        description="Extract invoice details via LlamaExtract",
        source=GithubTemplateRepo(
            url="https://github.com/run-llama/template-workflow-invoice-extraction"
        ),
        llama_cloud=True,
    ),
    TemplateOption(
        id="rag",
        name="RAG",
        description="Embed, index, and query documents (RAG pipeline)",
        source=GithubTemplateRepo(
            url="https://github.com/run-llama/template-workflow-rag"
        ),
        llama_cloud=False,
    ),
    TemplateOption(
        id="web_scraping",
        name="Web Scraping",
        description="Scrape and summarize URLs via Gemini API",
        source=GithubTemplateRepo(
            url="https://github.com/run-llama/template-workflow-web-scraping"
        ),
        llama_cloud=False,
    ),
]

ALL_TEMPLATES = UI_TEMPLATES + HEADLESS_TEMPLATES
