"""
Microserviço FastAPI — Extração Determinística de NCMs
=======================================================
Recebe um PDF via POST (base64 ou upload) e retorna NCMs
estruturados extraídos de forma determinística com pdfplumber.

O LLM só é chamado como fallback se nenhuma tabela for detectada.

Instalar:
    pip install fastapi uvicorn pdfplumber python-multipart httpx

Rodar local:
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload

Deploy Railway/Render:
    Adicione Procfile: web: uvicorn main:app --host 0.0.0.0 --port $PORT
"""

import re
import io
import base64
import logging
from typing import Optional

# FastAPI e dependências
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Extração de PDF
import pdfplumber

# HTTP para chamar OpenAI (fallback)
import httpx

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="Extrator NCM", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Configuração ────────────────────────────────────────────────────────────

OPENAI_API_KEY = ""   # Preencha ou use variável de ambiente: import os; os.getenv("OPENAI_API_KEY")
USAR_LLM_FALLBACK = True  # False desativa o fallback por LLM

# ── Schemas de entrada/saída ────────────────────────────────────────────────

class ExtrairRequest(BaseModel):
    pdf_base64: str          # PDF codificado em base64
    file_name: str = "arquivo.pdf"
    folder_name: str = ""    # Nome da pasta no Drive (ajuda na classificação)
    forcar_regime: Optional[str] = None  # "icms_st" | "monofasico" | None = auto

class NCMItem(BaseModel):
    regime: str              # "icms_st" | "monofasico"
    ativo: bool
    ncm: str                 # só dígitos, 4-8 chars
    descricao: Optional[str]
    cest: Optional[str]      # icms_st
    segmento: Optional[str]
    cst: Optional[str]       # icms_st
    cfop_interno: Optional[str]
    cfop_interestadual: Optional[str]
    cst_pis: Optional[str]   # monofasico
    cst_cofins: Optional[str]
    natureza_receita: Optional[str]
    norma_legal: Optional[str]
    fonte_pdf: str
    metodo_extracao: str     # "tabela" | "texto" | "llm_fallback"

class ExtrairResponse(BaseModel):
    items: list[NCMItem]
    total: int
    metodo_principal: str
    arquivo: str
    avisos: list[str]

# ── Detecção de regime pelo contexto ───────────────────────────────────────

PADROES_MONOFASICO = [
    r"monof[aá]sico",
    r"pis[/\s]+cofins",
    r"tributa[çc][aã]o\s+monof[aá]sica",
    r"alíquota\s+zero",
    r"regime\s+monof",
    r"art\.?\s*2[12345].*lei.*10\.8[56][0-9]",  # Leis do monofásico
    r"lei\s+10\.147",
    r"lei\s+10\.485",
    r"lei\s+10\.560",
    r"cst[_\s]*0[24]",    # CST 02 e 04 são monofásico
    r"natureza\s+de\s+receita",
]

PADROES_ICMS_ST = [
    r"substitui[çc][aã]o\s+tribut[aá]ria",
    r"icms[_\s\-]+st",
    r"sujeito\s+passivo\s+por\s+substitui[çc][aã]o",
    r"cest\s*[:\s]*\d{2}\.\d{3}\.\d{2}",
    r"protocolo\s+icms",
    r"conv[eê]nio\s+icms",
    r"mva|margem\s+de\s+valor\s+agregado",
    r"pauta\s+fiscal",
    r"cfop\s+5405|cfop\s+6404",
]


def detectar_regime(texto: str, folder_name: str = "") -> tuple[str, float]:
    """
    Retorna (regime, confiança 0-1).
    Analisa nome da pasta + conteúdo do texto.
    """
    texto_lower = texto.lower()
    folder_lower = folder_name.lower()

    # 1. Nome da pasta é a fonte mais confiável
    if re.search(r"\bmono[_\s]|monofas", folder_lower):
        return "monofasico", 1.0
    if re.search(r"\bicms[_\s]*st[_\s]|^st[_\s]", folder_lower):
        return "icms_st", 1.0

    # 2. Pontuação pelo conteúdo
    score_mono = sum(1 for p in PADROES_MONOFASICO if re.search(p, texto_lower))
    score_st   = sum(1 for p in PADROES_ICMS_ST   if re.search(p, texto_lower))

    total = score_mono + score_st
    if total == 0:
        return "icms_st", 0.4   # default conservador com baixa confiança

    if score_mono > score_st:
        conf = score_mono / total
        return "monofasico", round(conf, 2)
    else:
        conf = score_st / total
        return "icms_st", round(conf, 2)


# ── Extração de NCMs de tabelas ─────────────────────────────────────────────

# Regex para NCM: 4 a 8 dígitos, com ou sem pontos/hífen
RE_NCM = re.compile(r"\b(\d{4}[\.\-]?\d{0,2}[\.\-]?\d{0,2}[\.\-]?\d{0,2})\b")

# Regex para CEST: formato XX.XXX.XX
RE_CEST = re.compile(r"\b(\d{2}\.\d{3}\.\d{2})\b")

# Títulos de seções/anexos que definem o segmento e regime
RE_TITULO_SECAO = re.compile(
    r"(an[ea]xo\s+[IVXLC]+|cap[íi]tulo\s+\w+|se[çc][aã]o\s+\w+|"
    r"produto[s]?\s+sujeito[s]?|substitui[çc][aã]o\s+tribut|monof[aá]sico)",
    re.IGNORECASE
)

def limpar_ncm(raw: str) -> Optional[str]:
    """Remove pontuação e valida NCM (4-8 dígitos)."""
    digits = re.sub(r"[^\d]", "", raw)
    if 4 <= len(digits) <= 8:
        return digits
    return None


def extrair_de_tabela(page, regime: str, segmento: str, fonte: str) -> list[dict]:
    """
    Extrai NCMs de tabelas detectadas pelo pdfplumber em uma página.
    Cada linha da tabela é um potencial registro.
    """
    items = []
    tables = page.extract_tables()
    if not tables:
        return items

    for table in tables:
        if not table or len(table) < 2:
            continue

        # Detecta colunas: procura cabeçalho com NCM, CEST, Descrição
        header = [str(c).lower().strip() if c else "" for c in (table[0] or [])]
        col_ncm   = next((i for i, h in enumerate(header) if "ncm" in h or "sh" in h), None)
        col_cest  = next((i for i, h in enumerate(header) if "cest" in h), None)
        col_desc  = next((i for i, h in enumerate(header) if "descri" in h or "produto" in h or "mercad" in h), None)
        col_item  = next((i for i, h in enumerate(header) if "item" in h), None)

        # Se não encontrou coluna NCM no header, tenta heurística nas células
        if col_ncm is None:
            # Procura qual coluna tem mais matches de NCM
            contagens = []
            for ci in range(len(header)):
                vals = [str(row[ci]) for row in table[1:] if row and ci < len(row) and row[ci]]
                hits = sum(1 for v in vals if RE_NCM.search(v))
                contagens.append(hits)
            if contagens and max(contagens) > 0:
                col_ncm = contagens.index(max(contagens))

        if col_ncm is None:
            log.debug("Tabela sem coluna NCM identificável — pulando")
            continue

        for row in table[1:]:
            if not row or all(not c for c in row):
                continue

            raw_ncm  = str(row[col_ncm]).strip() if col_ncm < len(row) and row[col_ncm] else ""
            raw_cest = str(row[col_cest]).strip() if col_cest and col_cest < len(row) and row[col_cest] else ""
            raw_desc = str(row[col_desc]).strip() if col_desc and col_desc < len(row) and row[col_desc] else ""

            # Pode haver múltiplos NCMs na célula (ex: "3208\n3209\n3210")
            ncms_brutos = RE_NCM.findall(raw_ncm) if raw_ncm else []

            # Se célula NCM está vazia, tenta extrair de toda a linha
            if not ncms_brutos:
                linha_str = " ".join(str(c) for c in row if c)
                ncms_brutos = RE_NCM.findall(linha_str)

            for ncm_raw in ncms_brutos:
                ncm = limpar_ncm(ncm_raw)
                if not ncm:
                    continue

                # CEST
                cest_match = RE_CEST.search(raw_cest) if raw_cest else None
                cest = cest_match.group(1) if cest_match else None
                if not cest:
                    linha_str = " ".join(str(c) for c in row if c)
                    cest_match = RE_CEST.search(linha_str)
                    cest = cest_match.group(1) if cest_match else None

                item = {
                    "regime": regime,
                    "ativo": True,
                    "ncm": ncm,
                    "descricao": raw_desc[:500] if raw_desc else None,
                    "cest": cest,
                    "segmento": segmento,
                    "cst": "060" if regime == "icms_st" else None,
                    "cfop_interno": "5405" if regime == "icms_st" else None,
                    "cfop_interestadual": "6404" if regime == "icms_st" else None,
                    "cst_pis": "04" if regime == "monofasico" else None,
                    "cst_cofins": "04" if regime == "monofasico" else None,
                    "natureza_receita": None,
                    "norma_legal": None,
                    "fonte_pdf": fonte,
                    "metodo_extracao": "tabela",
                }
                items.append(item)

    return items


def extrair_de_texto_livre(texto: str, regime: str, segmento: str, fonte: str) -> list[dict]:
    """
    Fallback: extrai NCMs mencionados no texto corrido.
    Menos preciso — usa contexto de sentença para enriquecer.
    """
    items = []
    linhas = texto.split("\n")

    for i, linha in enumerate(linhas):
        ncms = RE_NCM.findall(linha)
        for ncm_raw in ncms:
            ncm = limpar_ncm(ncm_raw)
            if not ncm:
                continue

            # Contexto: 2 linhas ao redor para capturar descrição e CEST
            contexto = " ".join(linhas[max(0, i-1):i+2])
            cest_match = RE_CEST.search(contexto)

            # Tenta extrair descrição: texto após o NCM na mesma linha
            partes = linha.split(ncm_raw, 1)
            descricao = partes[1].strip()[:300] if len(partes) > 1 else None

            item = {
                "regime": regime,
                "ativo": True,
                "ncm": ncm,
                "descricao": descricao or None,
                "cest": cest_match.group(1) if cest_match else None,
                "segmento": segmento,
                "cst": "060" if regime == "icms_st" else None,
                "cfop_interno": "5405" if regime == "icms_st" else None,
                "cfop_interestadual": "6404" if regime == "icms_st" else None,
                "cst_pis": "04" if regime == "monofasico" else None,
                "cst_cofins": "04" if regime == "monofasico" else None,
                "natureza_receita": None,
                "norma_legal": None,
                "fonte_pdf": fonte,
                "metodo_extracao": "texto",
            }
            items.append(item)

    return items


def detectar_segmento(texto: str) -> str:
    """
    Infere o segmento a partir do texto: título do anexo ou palavras-chave.
    """
    texto_lower = texto[:3000].lower()  # Analisa começo do doc
    mapeamento = {
        "Autopeças": ["autopeça", "autopeca", "veículo", "motor", "freio"],
        "Combustíveis e Lubrificantes": ["combustível", "gasolina", "diesel", "glp", "lubrificante"],
        "Medicamentos": ["medicamento", "farmacê", "farmace", "princípio ativo"],
        "Produtos Alimentícios": ["aliment", "bebida", "laticínio", "laticinio", "chocolate"],
        "Perfumaria e Higiene": ["perfum", "cosmétic", "cosmetic", "higiene", "xampu"],
        "Eletrônicos": ["eletrônico", "eletronico", "smartphone", "televisor"],
        "Materiais de Construção": ["construção", "construcao", "cimento", "tijolo"],
        "Materiais de Limpeza": ["limpeza", "detergente", "sabão", "sabao"],
        "Pneumáticos": ["pneumático", "pneu", "câmara de ar"],
        "Tintas e Vernizes": ["tinta", "verniz"],
    }
    for seg, palavras in mapeamento.items():
        if any(p in texto_lower for p in palavras):
            return seg
    return "Não identificado"


def deduplicar(items: list[dict]) -> list[dict]:
    """Remove NCMs duplicados, mantendo o com mais campos preenchidos."""
    seen = {}
    for item in items:
        key = (item["ncm"], item["regime"])
        if key not in seen:
            seen[key] = item
        else:
            # Mantém o que tiver mais campos não-nulos
            campos_novos = sum(1 for v in item.values() if v is not None)
            campos_atual = sum(1 for v in seen[key].values() if v is not None)
            if campos_novos > campos_atual:
                seen[key] = item
    return list(seen.values())


# ── Fallback LLM ─────────────────────────────────────────────────────────────

async def extrair_via_llm(texto: str, regime: str, fonte: str) -> list[dict]:
    """
    Chama GPT-4o-mini somente quando pdfplumber não encontrou tabelas.
    Prompt mais simples e robusto — sem exigir campos raros.
    """
    if not OPENAI_API_KEY or not USAR_LLM_FALLBACK:
        return []

    schema = {
        "icms_st":    '{"regime":"icms_st","ncm":"XXXXXXXX","descricao":"...","cest":"XX.XXX.XX ou null"}',
        "monofasico": '{"regime":"monofasico","ncm":"XXXXXXXX","descricao":"...","cst_pis":"04","cst_cofins":"04"}',
    }[regime]

    prompt = f"""Extraia TODOS os códigos NCM do texto abaixo.
Regime: {regime.upper()}
Retorne APENAS um array JSON válido. Sem markdown. Sem explicações.
Schema de cada item: {schema}
Se não houver NCMs, retorne [].

Texto:
{texto[:8000]}"""

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
                json={
                    "model": "gpt-4o-mini",
                    "max_tokens": 4000,
                    "messages": [
                        {"role": "system", "content": "Você extrai NCMs de textos legislativos. Responda apenas com JSON."},
                        {"role": "user",   "content": prompt},
                    ],
                },
            )
        raw = resp.json()["choices"][0]["message"]["content"].strip()
        raw = re.sub(r"```json|```", "", raw).strip()
        parsed = __import__("json").loads(raw)

        items = []
        for p in (parsed if isinstance(parsed, list) else []):
            ncm = limpar_ncm(str(p.get("ncm", "")))
            if not ncm:
                continue
            items.append({
                "regime": regime,
                "ativo": True,
                "ncm": ncm,
                "descricao": p.get("descricao"),
                "cest": p.get("cest"),
                "segmento": None,
                "cst": None,
                "cfop_interno": "5405" if regime == "icms_st" else None,
                "cfop_interestadual": "6404" if regime == "icms_st" else None,
                "cst_pis": p.get("cst_pis", "04") if regime == "monofasico" else None,
                "cst_cofins": p.get("cst_cofins", "04") if regime == "monofasico" else None,
                "natureza_receita": p.get("natureza_receita"),
                "norma_legal": None,
                "fonte_pdf": fonte,
                "metodo_extracao": "llm_fallback",
            })
        return items

    except Exception as e:
        log.error(f"Erro no fallback LLM: {e}")
        return []


# ── Endpoint principal ────────────────────────────────────────────────────────

@app.post("/extrair-ncms", response_model=ExtrairResponse)
async def extrair_ncms(req: ExtrairRequest):
    avisos = []

    # 1. Decodifica o PDF
    try:
        pdf_bytes = base64.b64decode(req.pdf_base64)
    except Exception:
        raise HTTPException(400, "pdf_base64 inválido")

    # 2. Abre com pdfplumber
    all_items = []
    texto_completo = []
    paginas_com_tabela = 0

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            log.info(f"PDF '{req.file_name}' — {len(pdf.pages)} páginas")

            for num_pag, page in enumerate(pdf.pages, 1):
                texto_pag = page.extract_text() or ""
                texto_completo.append(texto_pag)

                # Detecta regime e segmento nesta página
                regime_pag, conf = detectar_regime(
                    texto_pag,
                    req.folder_name if num_pag == 1 else ""
                )
                if req.forcar_regime:
                    regime_pag = req.forcar_regime

                segmento = detectar_segmento(texto_pag)

                # Tentativa 1: extração de tabelas (determinístico)
                items_tabela = extrair_de_tabela(page, regime_pag, segmento, req.file_name)
                if items_tabela:
                    paginas_com_tabela += 1
                    all_items.extend(items_tabela)
                else:
                    # Tentativa 2: extração de texto corrido
                    items_texto = extrair_de_texto_livre(texto_pag, regime_pag, segmento, req.file_name)
                    all_items.extend(items_texto)

    except Exception as e:
        raise HTTPException(500, f"Erro ao processar PDF: {e}")

    # 3. Fallback LLM se nenhuma extração funcionou
    texto_unido = "\n".join(texto_completo)
    metodo_principal = "tabela" if paginas_com_tabela > 0 else "texto"

    if not all_items and USAR_LLM_FALLBACK:
        avisos.append("Nenhuma tabela/NCM detectado — acionando fallback LLM")
        regime_doc, _ = detectar_regime(texto_unido, req.folder_name)
        if req.forcar_regime:
            regime_doc = req.forcar_regime
        llm_items = await extrair_via_llm(texto_unido, regime_doc, req.file_name)
        all_items.extend(llm_items)
        metodo_principal = "llm_fallback"

    if not all_items:
        avisos.append("Nenhum NCM extraído — PDF pode ser escaneado ou sem dados estruturados")

    # 4. Deduplicação e validação final
    all_items = deduplicar(all_items)

    # Filtra NCMs obviamente inválidos (menos de 4 dígitos)
    all_items = [i for i in all_items if len(i["ncm"]) >= 4]

    log.info(
        f"'{req.file_name}' → {len(all_items)} NCMs | "
        f"método: {metodo_principal} | "
        f"páginas com tabela: {paginas_com_tabela}"
    )

    return ExtrairResponse(
        items=[NCMItem(**i) for i in all_items],
        total=len(all_items),
        metodo_principal=metodo_principal,
        arquivo=req.file_name,
        avisos=avisos,
    )


@app.get("/health")
def health():
    return {"status": "ok", "versao": "1.0.0"}


# ── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
