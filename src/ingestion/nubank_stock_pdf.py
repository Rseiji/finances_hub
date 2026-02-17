from __future__ import annotations

import argparse
import os
import re
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from io import StringIO
from typing import Callable

import pandas as pd

from ingestion.models import PdfTradeEvent
from storage.raw_postgres import insert_pdf_trade_events

PdfTextExtractor = Callable[[str], str]
LlmCaller = Callable[[str], str]
EXPECTED_TABLE_COLUMNS = [
    "Mercado",
    "C/V",
    "Tipo de Mercado",
    "Especificação do Título",
    "Observação",
    "Quantidade",
    "Preço/Ajuste",
    "Valor/Ajuste",
    "D/C",
]


@dataclass(frozen=True)
class TableExtractionConfig:
    expected_columns: list[str] | None = None
    system_prompt: str = "You are a financial document parser."
    no_table_token: str = "NO_TABLE_FOUND"
    collapse_repeated_chars: bool = False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract tabular data from a PDF using an LLM."
    )
    parser.add_argument(
        "--pdf_path",
        help="Path to the PDF file"
    )
    parser.add_argument(
        "--model",
        help="Groq model id",
        default=None
    )
    parser.add_argument(
        "--expected-columns",
        nargs="*",
        default=EXPECTED_TABLE_COLUMNS,
        help="Optional list of expected column names for prompt guidance",
    )
    parser.add_argument(
        "--system-prompt",
        default=TableExtractionConfig().system_prompt
    )
    parser.add_argument(
        "--collapse-repeated-chars",
        action="store_true"
    )
    parser.add_argument(
        "--taxa-liquidacao",
        default="0",
        help="Taxa de liquidacao (BRL) for the statement."
    )
    parser.add_argument(
        "--emolumentos",
        default="0",
        help="Emolumentos (BRL) for the statement."
    )
    parser.add_argument(
        "--taxa-transf-ativos",
        default="0",
        help="Taxa de transferencia de ativos (BRL) for the statement."
    )
    args = parser.parse_args()

    llm_call = make_groq_caller(model=args.model, system_prompt=args.system_prompt)
    config = TableExtractionConfig(
        expected_columns=args.expected_columns,
        system_prompt=args.system_prompt,
        collapse_repeated_chars=args.collapse_repeated_chars,
    )

    df = extract_table_from_pdf(args.pdf_path, llm_call, config=config)

    if df is None:
        print(config.no_table_token)
        return

    print(df.to_string(index=False))

    answer = input("Append data to bronze.pdf_nuinvest_trade_events? (y/N): ").strip()
    if answer.lower() != "y":
        print("Aborted by user.")
        return

    events = _build_trade_events(
        df,
        pdf_path=args.pdf_path,
        model=args.model,
        taxa_liquidacao=_parse_decimal(args.taxa_liquidacao),
        emolumentos=_parse_decimal(args.emolumentos),
        taxa_transf_ativos=_parse_decimal(args.taxa_transf_ativos),
    )
    inserted = insert_pdf_trade_events(events)
    print(f"Inserted {inserted} rows into bronze.pdf_nuinvest_trade_events.")


def make_groq_caller(
    api_key: str | None = None,
    model: str | None = None,
    temperature: float = 0.0,
    system_prompt: str = TableExtractionConfig().system_prompt,
) -> LlmCaller:
    try:
        from groq import Groq
        from tenacity import retry, stop_after_attempt, wait_random_exponential
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise ImportError("groq and tenacity are required to use make_groq_caller") from exc

    key = api_key or os.environ.get("GROQ_API_KEY")
    if not key:
        raise ValueError("GROQ_API_KEY is required to use make_groq_caller")

    client = Groq(api_key=key)
    resolved_model = model or _get_best_groq_model(client)

    @retry(wait=wait_random_exponential(min=1, max=8), stop=stop_after_attempt(3))
    def _call(prompt: str) -> str:
        response = client.chat.completions.create(
            model=resolved_model,
            temperature=temperature,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],
        )
        return response.choices[0].message.content.strip()

    return _call


def _get_best_groq_model(client: object) -> str:
    models = client.models.list().data
    priority = ["70b", "mixtral", "8b", "gemma"]
    for preferred in priority:
        for model in models:
            if preferred in model.id.lower():
                return model.id
    return models[0].id


def extract_table_from_pdf(
    pdf_path: str,
    llm_call: LlmCaller,
    config: TableExtractionConfig | None = None,
    text_extractor: PdfTextExtractor | None = None,
) -> pd.DataFrame | None:
    extractor = text_extractor or _default_pdf_text_extractor
    text = extractor(pdf_path)
    return extract_table_from_text(text, llm_call, config=config)


def _default_pdf_text_extractor(pdf_path: str) -> str:
    try:
        import pdfplumber
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise ImportError("pdfplumber is required to extract text from PDF files") from exc

    with pdfplumber.open(pdf_path) as pdf:
        return "\n".join(page.extract_text() or "" for page in pdf.pages)


def extract_table_from_text(
    text: str,
    llm_call: LlmCaller,
    config: TableExtractionConfig | None = None,
) -> pd.DataFrame | None:
    resolved_config = config or TableExtractionConfig()
    cleaned_text = _normalize_pdf_text(text, resolved_config)

    prompt = _build_prompt(cleaned_text, resolved_config)
    csv_text = llm_call(prompt).strip()
    if csv_text == resolved_config.no_table_token:
        return None
    return _parse_csv(csv_text, resolved_config)


def _parse_csv(csv_text: str, config: TableExtractionConfig) -> pd.DataFrame:
    df = pd.read_csv(StringIO(csv_text))

    if config.expected_columns:
        if len(config.expected_columns) == df.shape[1]:
            df.columns = config.expected_columns
            return df

    first_row = df.iloc[0].astype(str).tolist()

    if all(any(c.isalpha() for c in cell) for cell in first_row):
        df = pd.read_csv(StringIO(csv_text))
        return df

    return df


def _normalize_pdf_text(text: str, config: TableExtractionConfig) -> str:
    if config.collapse_repeated_chars:
        # Remove duplicated characters (AA -> A)
        text = re.sub(r"(.)\1+", r"\1", text)

    # Remove duplicated spaces
    text = re.sub(r'\s+', ' ', text)

    # Normalize linebreaks
    text = re.sub(r' *\n *', '\n', text)

    return text.strip()


def _build_prompt(text: str, config: TableExtractionConfig) -> str:
    expected_columns = ""
    if config.expected_columns:
        expected_columns = "\nExpected columns: " + ", ".join(config.expected_columns)

    return f"""
        Você é um parser especializado em extração de tabelas.

        Analise o texto e identifique tabelas estruturadas.

        Extraia a tabela principal e converta para CSV.

        Regras:

        - Preserve exatamente colunas e ordem
        - Reconstrua linhas quebradas
        - Ignore texto fora da tabela
        - Não invente dados
        - Não resuma
        - Não explique

        Formato:

        - Responda apenas CSV puro
        - vírgula como separador
        - decimal com ponto
        - sem milhar

        Se não houver tabela:
        NO_TABLE_FOUND

        {expected_columns}

        Texto:
        {text}
        """


def _build_trade_events(
    df: pd.DataFrame,
    pdf_path: str,
    model: str | None,
    taxa_liquidacao: Decimal,
    emolumentos: Decimal,
    taxa_transf_ativos: Decimal,
) -> list[PdfTradeEvent]:
    normalized_df = _normalize_trade_table(df)
    fetched_at = datetime.now(timezone.utc).isoformat()
    statement_date = _extract_statement_date_from_filename(pdf_path)
    file_name = os.path.basename(pdf_path)
    request_params = {
        "pdf_path": pdf_path,
        "model": model,
    }

    events = []
    for _, row in normalized_df.iterrows():
        dc_value = str(row["D/C"]).strip().upper()[:1]
        event = PdfTradeEvent(
            uid=str(uuid.uuid4()),
            source="nubank_pdf",
            request_params=request_params,
            fetched_at=fetched_at,
            mercado=str(row["Mercado"]).strip(),
            cv=str(row["C/V"]).strip(),
            tipo_mercado=_clean_text(row["Tipo de Mercado"]),
            espec_titulo=_clean_text(row["Especificação do Título"]),
            observacao=_clean_text(row["Observação"]),
            quantidade=_parse_int(row["Quantidade"]),
            preco=_parse_decimal(row["Preço/Ajuste"]),
            valor=_parse_decimal(row["Valor/Ajuste"]),
            dc=dc_value,
            taxa_liquidacao=taxa_liquidacao,
            emolumentos=emolumentos,
            taxa_transf_ativos=taxa_transf_ativos,
            file_path=pdf_path,
            file_name=file_name,
            statement_date=statement_date,
        )
        events.append(event)
    return events


def _normalize_trade_table(df: pd.DataFrame) -> pd.DataFrame:
    if list(df.columns) == EXPECTED_TABLE_COLUMNS:
        return df

    normalized_columns = {}
    for column in df.columns:
        normalized = _normalize_header(str(column))
        normalized_columns[column] = normalized

    lookup = {
        _normalize_header(name): name for name in EXPECTED_TABLE_COLUMNS
    }

    resolved = {}
    for original, normalized in normalized_columns.items():
        if normalized in lookup:
            resolved[original] = lookup[normalized]

    if resolved:
        df = df.rename(columns=resolved)

    missing = [col for col in EXPECTED_TABLE_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    return df[EXPECTED_TABLE_COLUMNS]


def _normalize_header(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", value.strip().lower())
    cleaned = cleaned.replace("/", " ")
    cleaned = re.sub(r"[^a-z0-9 ]", "", cleaned)
    return cleaned


def _clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def _parse_int(value: object) -> int:
    if pd.isna(value):
        return 0
    text = re.sub(r"[^0-9-]", "", str(value))
    return int(text) if text else 0


def _parse_decimal(value: object) -> Decimal:
    if pd.isna(value):
        return Decimal("0")
    text = str(value).strip()
    if not text:
        return Decimal("0")
    normalized = re.sub(r"[^0-9,.-]", "", text)
    if "," in normalized and "." in normalized:
        normalized = normalized.replace(".", "").replace(",", ".")
    elif "," in normalized and "." not in normalized:
        normalized = normalized.replace(",", ".")
    normalized = re.sub(r"[^0-9.-]", "", normalized)
    return Decimal(normalized or "0")


def _extract_statement_date_from_filename(pdf_path: str) -> date | None:
    file_name = os.path.basename(pdf_path)
    match = re.search(r"(\d{4})[-_/]?(\d{2})[-_/]?(\d{2})", file_name)
    if not match:
        return None
    year, month, day = match.groups()
    try:
        return date(int(year), int(month), int(day))
    except ValueError:
        return None


if __name__ == "__main__":
    main()
