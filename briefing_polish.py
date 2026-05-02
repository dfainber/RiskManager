"""
briefing_polish.py — LLM-as-editor wrapper for fund briefings.

Pattern: deterministic rule-based generator runs first (source of truth for
numbers); Haiku 4.5 then polishes the prose for flow / brevity / emphasis.
Numbers and fund names come from the rule output — Haiku is forbidden from
introducing new ones.

Activated by env var `USE_LLM_BRIEFING_POLISH=1`. If anything fails (network,
API error, validation), the deterministic text is returned unchanged.
"""
from __future__ import annotations

import os
import re

# Lazy import — anthropic SDK only loaded when polish actually fires
_client = None


def _get_client():
    global _client
    if _client is None:
        import anthropic
        _client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from env
    return _client


_POLISH_ENABLED_CACHE: bool | None = None


def _polish_enabled() -> bool:
    global _POLISH_ENABLED_CACHE
    if _POLISH_ENABLED_CACHE is None:
        flag = os.environ.get("USE_LLM_BRIEFING_POLISH", "").strip().lower()
        _POLISH_ENABLED_CACHE = flag in ("1", "true", "yes", "on")
    return _POLISH_ENABLED_CACHE


_SYSTEM_PROMPT = """Você é um editor de risco da Galápagos Capital revisando briefings curtos por fundo do Morning Call.

Cada briefing chega com um parágrafo de comentário (1 a 3 frases) gerado por regras determinísticas. Os números e fatos já estão corretos — sua tarefa é editar o texto para:
- Melhorar fluidez e clareza
- Eliminar redundâncias e palavras ocas
- Priorizar o que importa primeiro
- Manter tom direto, técnico, profissional

REGRAS RÍGIDAS — violar qualquer uma significa fallback ao texto original:

1. Preserve TODAS as tags HTML (`<b>`, `<span>`, `<i>`, etc.) exatamente como vêm. Não adicione, remova, renomeie ou aninhe tags.

2. NÃO INVENTE NÚMEROS. Não adicione percentuais, basis points, ratios, anos, ou qualquer valor numérico que não esteja no texto original. Não altere sinais (+/-) nem unidades (%, bps, yr).

3. NÃO INVENTE NOMES. Não introduza nomes de fundos, produtos, PMs, livros ou benchmarks que não estejam no texto.

4. NÃO ADICIONE recomendações, opiniões, previsões ou ações sugeridas ("recomendamos…", "deveríamos…", "atenção a…") que não estejam no original. O briefing é descritivo, não prescritivo.

5. PODE: reordenar frases, trocar conectores, remover repetição, encurtar ou unir frases, escolher sinônimos mais precisos.

6. SAÍDA: APENAS o HTML revisado. Sem preâmbulo ("Aqui está…"), sem explicação, sem cerca de markdown, sem aspas envolventes.

7. COMPRIMENTO: igual ou menor que o original. Nunca expanda.

8. IDIOMA: português do Brasil. Termos técnicos em inglês (gross, beta, long, short, hedge) podem ficar como estão se já vierem em inglês.

EXEMPLOS:

INPUT (fund=MACRO):
Dia com alpha do dia em <b>+0.45%</b> e VaR ↑ <b>8 bps</b> vs D-1. Fluxo: contrib <b>NTN-B 2030</b> <span style="color:var(--up)">+0.32%</span> · detrator <b>DI1F26</b> <span style="color:var(--down)">-0.18%</span>. Posicionamento: util VaR em 78% — vigilância; dado em Juros Reais (IPCA) (+1.20 yr eq); PM CI com apenas 12 bps de stop.

OUTPUT:
Alpha de <b>+0.45%</b> no dia, VaR subiu <b>8 bps</b>. <b>NTN-B 2030</b> <span style="color:var(--up)">+0.32%</span> contribuiu, <b>DI1F26</b> <span style="color:var(--down)">-0.18%</span> detraiu. Util VaR em 78%, dado em Juros Reais (IPCA) com +1.20 yr eq; CI com apenas 12 bps de stop.

INPUT (fund=ALBATROZ):
Dia neutro — alpha e risco estáveis, nada material para reportar.

OUTPUT:
Dia neutro — alpha e risco estáveis.

INPUT (fund=EVOLUTION):
Dia com alpha do dia em <b>-0.22%</b>. Fluxo: detrator <b>BOVA11</b> <span style="color:var(--down)">-0.15%</span>. Posicionamento: longo em Equity BR (+8.4% NAV).

OUTPUT:
Alpha de <b>-0.22%</b>, puxado por <b>BOVA11</b> <span style="color:var(--down)">-0.15%</span>. Long Equity BR em +8.4% NAV.

INPUT (fund=FRONTIER):
Sem PA disponível no dia; olhar apenas VaR e exposições abaixo. Posicionamento: alocação equity <b>92%</b>, β ponderado <b>1.08</b>.

OUTPUT:
PA indisponível hoje — referência apenas em VaR e exposição. Alocação equity <b>92%</b>, β ponderado <b>1.08</b>."""


# Pattern that captures any signed numeric token: 1.5, +1.5, -0.32, 12, 100, 1.234.567,89
# Used to validate that the polished output doesn't introduce new numbers.
_NUM_RE = re.compile(r"[-+]?\d[\d.,]*")


def _extract_numbers(s: str) -> set[str]:
    """Return the set of numeric tokens in ``s``, normalized.

    Strips trailing punctuation and unit-adjacent characters so that "12 bps"
    and "12 bps." both contribute "12".
    """
    return {tok.strip(".,") for tok in _NUM_RE.findall(s) if tok.strip(".,")}


def polish_commentary(text: str, fund_short: str) -> str:
    """Polish a fund briefing commentary via Haiku 4.5.

    Returns the original ``text`` unchanged if the env flag is off, the SDK
    is unavailable, the API call fails, or the polished output fails any
    guardrail (empty, too long, introduces a new number).
    """
    if not _polish_enabled() or not text or not text.strip():
        return text

    try:
        client = _get_client()
        resp = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=500,
            system=[{
                "type": "text",
                "text": _SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }],
            messages=[{
                "role": "user",
                "content": f"Fundo: {fund_short}\n\nPolir o seguinte briefing:\n\n{text}",
            }],
        )
        polished = next(
            (b.text for b in resp.content if b.type == "text"), ""
        ).strip()
    except Exception as e:
        # Network error, auth error, rate limit — fall back silently.
        # Set ANTHROPIC_LOG=info to surface in stderr if needed.
        if os.environ.get("LLM_POLISH_DEBUG"):
            print(f"  [polish] {fund_short}: API error {type(e).__name__}: {e}")
        return text

    if not polished:
        return text

    # Guardrail 1: length cap — polished must not exceed 1.2× original
    if len(polished) > 1.2 * len(text):
        if os.environ.get("LLM_POLISH_DEBUG"):
            print(f"  [polish] {fund_short}: rejected (too long: {len(polished)} > {1.2*len(text):.0f})")
        return text

    # Guardrail 2: no new numbers — polished's numeric set must be subset of original's
    src_nums = _extract_numbers(text)
    out_nums = _extract_numbers(polished)
    new_nums = out_nums - src_nums
    if new_nums:
        if os.environ.get("LLM_POLISH_DEBUG"):
            print(f"  [polish] {fund_short}: rejected (new numbers: {new_nums})")
        return text

    return polished
