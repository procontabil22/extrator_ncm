"""
Microserviço de Extração Fiscal v3.9
=====================================
Três camadas: Determinístico → Regex → Semântico (LLM)
Mapeado para a estrutura REAL das tabelas no Supabase.

v3.9 — Correção _run_upsert_monofasicos: preserva _motor_extra para o motor (2026-03-21):
        • BUG CORRIGIDO — _filtrar_payload_mono era aplicado sobre o lote
          ANTES de chamar _upsert_motor_tributario, removendo _motor_extra
          e deixando o motor tributário sem as alíquotas necessárias.
        • CORREÇÃO: lote original (com _motor_extra) vai para
          _upsert_motor_tributario; lote_mono (filtrado) vai para
          produtos_monofasicos. Duas variáveis distintas por iteração.
        • Também resolve o erro PGRST204 "Could not find the
          'aliq_cofins_presumido' column": o campo não vazava pelo
          _filtrar_payload_mono, mas pelo lote ser passado com _motor_extra
          contendo os campos antigos que eram injetados erroneamente.
        • Nenhuma outra lógica alterada.

v3.8 — produtos_monofasicos como cadastro base puro (2026-03-21):
        • DECISÃO ARQUITETURAL: produtos_monofasicos é cadastro base de NCMs.
          Alíquotas e regime_tributario pertencem às tabelas do motor tributário
          (produtos_tributacao_concentrada e produtos_aliquota_zero).
        • _SCHEMA_MONO: removidos aliq_pis_simples, aliq_cofins_simples,
          aliq_pis_presumido, aliq_cofins_presumido, aliq_pis_real,
          aliq_cofins_real e regime_tributario.
        • mk_mono(): removidos todos os campos de alíquota e regime.
          A função agora retorna apenas os campos do cadastro base.
        • _mapear_item_monofasico(): idem — sem alíquotas nem regime.
          Os campos de alíquota continuam sendo lidos do JSON e repassados
          para _upsert_motor_tributario() via _preparar_payload_motor(),
          mas NÃO são mais enviados para produtos_monofasicos.
        • _filtrar_payload_mono(): garante que nenhum campo excluído
          escoe para o upsert em produtos_monofasicos.
        • Nenhuma outra lógica alterada.
        • O espelho nas tabelas do motor tributário (v3.7) continua intacto.

v3.7 — Espelho paralelo nas novas tabelas do motor tributário (2026-03-20):
        • produtos_monofasicos: INTACTA — pipeline e banco inalterados.
        • NOVO: _rotear_registro_motor() — roteia cada registro para a tabela
          correta do motor tributário com base no cst_pis:
            cst_pis='04' → produtos_tributacao_concentrada
            cst_pis='06' → produtos_aliquota_zero
            outros CSTs  → ignorado (não entra nas tabelas do motor)
        • NOVO: _preparar_payload_motor() — adapta o payload do pipeline
          para o schema das novas tabelas (aliquota_por_pauta derivada de
          aliq_pis_presumido IS NULL, grupo_legal inferido do fonte_arquivo).
        • NOVO: _upsert_motor_tributario() — persiste em lote nas novas
          tabelas em paralelo ao upsert em produtos_monofasicos.
        • _run_upsert_monofasicos(): mantém upsert em produtos_monofasicos
          inalterado + chama _upsert_motor_tributario() após cada lote.
        • salvar(): mantém upsert em produtos_monofasicos inalterado
          + chama _upsert_motor_tributario() para os registros monofásicos.
        • Nenhuma outra lógica alterada.

v3.6 — Correções pipeline produtos_monofasicos (2026-03-20):
        • BUG 1 CORRIGIDO — descricao sempre "Sem descrição":
          _mapear_item_monofasico buscava chave "description" (inglês).
          Agora tenta "descricao" | "description" | "produto" | "mercadoria".
        • BUG 2 CORRIGIDO — base_legal e lei sempre null:
          Buscava só "legal_basis". Agora tenta "base_legal" | "legal_basis" |
          "fundamento_legal" | "fundamentacao" com fallback seguro.
        • BUG 3 CORRIGIDO — alíquotas presumido/real null em vez dos defaults:
          _mapear_item_monofasico não aplicava fallback quando os campos
          al_pis_maj_percent etc. estavam ausentes no JSON.
          Agora aplica fallback: pis_presumido=0.65, cofins_presumido=3.0,
          pis_real=1.65, cofins_real=7.6 (Simples Nacional permanece 0).
          NCMs com "var." (bebidas por pauta) continuam null — correto.
        • BUG 4 CORRIGIDO — cst_pis/cst_cofins não respeitavam CST 01 e 06:
          Campos cst_mono_04, cst e cst_pis agora todos tentados em ordem.
          CST 06 (alíquota zero) e CST 01 (tributável básico) preservados.
        • _segmento_por_grupo expandido: "cesta", "aeronave", "embarcação",
          "saúde", "farmacê", "cosmétic", "embalagem" adicionados.
        • mk_mono atualizado: aliq_pis_presumido=0.65, aliq_cofins_presumido=3.0
          (estava incorreto: 0.65 e 3.0 já eram corretos, mas CST_DESC["06"]
          não existia — adicionado).
        • CST_DESC expandido com CST "02" e "06".

v3.5 — Correção trigger normas_legais_versoes (2026-03-19):
        • normas_legais usa INSERT puro em vez de upsert
        • Evita disparo do trigger trigger_versionar_norma em UPDATE
        • Se norma já existe (23505), recupera UUID via SELECT
        • Resolve erro: duplicate key value violates unique constraint
          "normas_legais_versoes_norma_id_versao_key"

v3.4 — Correções de persistência relacional (2026-03-19):
        • CORREÇÃO 1 — _FIELD_ALIASES["anexos_normas"] adicionado:
          "anexo_fiscal_id" → "anexo_id"
          "norma_legal_id"  → "norma_id"
          "tipo_relacao"    → "papel"
          Resolve os 3 FAILED do log (campos ignorados por nome errado)
        • CORREÇÃO 2 — _UPSERT_CONFLICT["anexos_fiscais"] adicionado:
          on_conflict="sigla_anexo,estado"
          Evita duplicatas a cada re-execução do pipeline
        • CORREÇÃO 3 — _UPSERT_CONFLICT["normas_legais"] corrigido:
          Removido orgao_emissor (nullable → quebrava o upsert no Postgres)
          Novo valor: "tipo_norma,numero,ano"
        • CORREÇÃO 4 — _SCHEMA_COLS["anexos_normas"] expandido:
          Adicionados "anexo_fiscal_id" e "norma_legal_id" como campos
          aceitos (aliases resolvidos antes de chegar ao schema check)
        • CORREÇÃO 5 — _run_upsert_icms_jsons usa _GlobalResolver:
          Placeholders agora persistem entre arquivos na mesma execução
          (ex: norma inserida no arquivo A é resolvida no arquivo B)
        • Nenhuma outra lógica alterada — Farinha de Trigo continua OK

v3.3 — Correcao vigencia_inicio NOT NULL / aliq_cofins_presumido:
        • Ambas as colunas confirmadas como NEVER GENERATED via
          information_schema (verificado em 17/03/2026)
        • _GENERATED_COLS_MONO introduzida como guarda de segurança
        • _filtrar_payload_mono(): remove automaticamente qualquer coluna
          da lista _GENERATED_COLS_MONO antes do upsert

v3.1 — Correção aliq_pis_simples / aliq_cofins_simples:
        • Colunas convertidas de GENERATED ALWAYS AS para numeric normal

v3.0 — Pipeline independente para produtos_monofasicos via JSON

v2.9 — descricao_cst nula para cst_icms="60"

v2.8 — Resolver global entre arquivos para anexos_normas

v2.7 — NCM multi-valor explodido em linhas individuais
v2.6 — Idempotência e RLS em produtos_icms_st
v2.5 — Correção de regex _PH_RE (minúsculas e hífens)
v2.4 — Correções críticas de RLS, colisão de variável, aliases e normas_legais

──────────────────────────────────────────────────────────────────────────────
Estado atual das colunas de produtos_monofasicos (17/03/2026):
  GENERATED (nunca enviar):  — nenhuma —
  NORMAL    (enviar sempre):  aliq_pis_simples, aliq_cofins_simples,
                               aliq_pis_presumido, aliq_cofins_presumido,
                               aliq_pis_real, aliq_cofins_real
──────────────────────────────────────────────────────────────────────────────
"""

import os, re, io, json, base64, logging
from datetime import datetime
from typing import Optional, List

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%S"
)
log = logging.getLogger(__name__)

SUPABASE_URL            = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY            = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY", "")
OPENAI_API_KEY          = os.getenv("OPENAI_API_KEY", "")
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
DRIVE_FOLDER_ID         = os.getenv("DRIVE_FOLDER_ID", "")
DRIVE_FOLDER_ID_JSONS   = os.getenv("DRIVE_FOLDER_ID_JSONS", "")
DRIVE_FOLDER_ID_MONO    = os.getenv("DRIVE_FOLDER_ID_MONO", "")

try:    import pdfplumber;                         HAS_PDF     = True
except: HAS_PDF     = False
try:    import openpyxl;                           HAS_XLSX    = True
except: HAS_XLSX    = False
try:    import pandas as pd;                       HAS_PANDAS  = True
except: HAS_PANDAS  = False
try:    from docx import Document as Docx;         HAS_DOCX    = True
except: HAS_DOCX    = False
try:    from supabase import create_client;        HAS_SB      = True
except: HAS_SB      = False
try:    import httpx;                              HAS_HTTPX   = True
except: HAS_HTTPX   = False
try:
    from googleapiclient.discovery import build
    from google.oauth2 import service_account
    from googleapiclient.http import MediaIoBaseDownload
    HAS_GDRIVE = True
except: HAS_GDRIVE = False

app = FastAPI(title="Extrator Fiscal v3.9", version="3.9.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY) if HAS_SB and SUPABASE_URL and SUPABASE_KEY else None

if SUPABASE_KEY:
    _key_tipo = "service_role" if len(SUPABASE_KEY) > 200 else "anon (ATENÇÃO: pode falhar RLS)"
    log.info(f"Supabase key carregada: tipo={_key_tipo} len={len(SUPABASE_KEY)}")
else:
    log.warning("Supabase key NÃO configurada — pipeline desabilitado")


class ExtrairRequest(BaseModel):
    pdf_base64:    Optional[str] = None
    file_name:     str = "arquivo"
    file_type:     str = "pdf"
    folder_name:   str = ""
    forcar_regime: Optional[str] = None


RE_NCM       = re.compile(r'\b(\d{4}[\.\ -]?\d{0,2}[\.\ -]?\d{0,2}[\.\ -]?\d{0,2})\b')
RE_CEST      = re.compile(r'\b(\d{2}\.\d{3}\.\d{2})\b')
RE_CFOP      = re.compile(r'\b([56]\d{3})\b')
RE_VIGENCIA  = re.compile(r'(?:vigência|efeitos?|a partir de)\s*[:\s]*(\d{2}[\/\.\-]\d{2}[\/\.\-]\d{4})', re.I)
RE_REVOGACAO = re.compile(r'(?:revogad[ao]|até)\s*[:\s]*(\d{2}[\/\.\-]\d{2}[\/\.\-]\d{4})', re.I)
RE_CONV      = re.compile(r'[Cc]onvênio\s+ICMS\s+(?:n[oº]?\s*)?(\d+[/\-]\d+)')
RE_PROT      = re.compile(r'[Pp]rotocolo\s+ICMS\s+(?:n[oº]?\s*)?(\d+[/\-]\d+)')
RE_LEI       = re.compile(r'[Ll]ei\s+(?:[Cc]omplementar\s+)?(?:n[oº]?\s*)?(\d+[\.\ d]*[/\-]\d+)')
RE_DECRETO   = re.compile(r'[Dd]ecreto\s+(?:n[oº]?\s*)?(\d+[\.\ d]*[/\-]\d+)')
RE_NAT       = re.compile(r'\b([12]\d{2})\b')

CST_DESC = {
    "01":  "Tributável alíquota básica",
    "02":  "Monofásico — fabricante alíquota majorada",
    "04":  "Monofásico — revenda alíquota zero",
    "06":  "Alíquota zero",
    "07":  "Isento",
    "60":  "ICMS-ST cobrado anteriormente",
    "060": "ICMS-ST cobrado anteriormente",
    "500": "CSOSN — ST cobrado anteriormente",
}

# =============================================================================
# LOOKUP DE FUNDAMENTAÇÃO JURÍDICA POR NCM
# =============================================================================

def _carregar_lookup_drive() -> dict:
    if not HAS_GDRIVE:
        return {}

    folder_id = DRIVE_FOLDER_ID_MONO or DRIVE_FOLDER_ID_JSONS
    if not folder_id:
        log.warning("[motor] DRIVE_FOLDER_ID_MONO não configurado — lookup não carregado do Drive")
        return {}

    try:
        svc = _svc()
        if not svc:
            return {}

        resultado = (
            svc.files()
            .list(
                q=(
                    f"'{folder_id}' in parents "
                    f"and name = 'fundamentacao_lookup.json' "
                    f"and mimeType = 'application/json' "
                    f"and trashed = false"
                ),
                fields="files(id, name)",
                pageSize=1,
            )
            .execute()
            .get("files", [])
        )

        if not resultado:
            log.warning("[motor] fundamentacao_lookup.json não encontrado no Drive")
            return {}

        file_id = resultado[0]["id"]
        buf = io.BytesIO()
        dl  = MediaIoBaseDownload(buf, svc.files().get_media(fileId=file_id))
        done = False
        while not done:
            _, done = dl.next_chunk()

        dados  = json.loads(buf.getvalue().decode("utf-8"))
        lookup = dados.get("fundamentacao_por_ncm", {})
        log.info(f"[motor] fundamentacao_lookup carregado do Drive: {len(lookup)} NCMs")
        return lookup

    except Exception as e:
        log.warning(f"[motor] Falha ao carregar fundamentacao_lookup do Drive: {e}")
        return {}


def _carregar_lookup_fundamentacao() -> dict:
    lookup = _carregar_lookup_drive()
    if lookup:
        return lookup

    caminhos = [
        os.getenv("FUNDAMENTACAO_LOOKUP_PATH", ""),
        os.path.join(os.path.dirname(__file__), "fundamentacao_lookup.json"),
        "fundamentacao_lookup.json",
    ]
    for caminho in caminhos:
        if caminho and os.path.isfile(caminho):
            try:
                with open(caminho, encoding="utf-8") as f:
                    dados = json.load(f)
                lookup = dados.get("fundamentacao_por_ncm", {})
                log.info(f"[motor] fundamentacao_lookup carregado localmente: {len(lookup)} NCMs de '{caminho}'")
                return lookup
            except Exception as e:
                log.warning(f"[motor] Falha ao carregar fundamentacao_lookup local '{caminho}': {e}")

    log.warning("[motor] fundamentacao_lookup não encontrado — campo fundamentacao_resumida ficará NULL")
    return {}


_FUNDAMENTACAO_LOOKUP: dict = _carregar_lookup_fundamentacao()

PADROES_MONO = [
    r"monof[aá]sico", r"pis[/\s]+cofins", r"lei\s+10\.147",
    r"lei\s+10\.485", r"lei\s+10\.560", r"natureza\s+de\s+receita",
    r"alíquota\s+zero", r"cst[_\s]*0[24]",
]
PADROES_ST = [
    r"substitui[çc][aã]o\s+tribut", r"icms[_\s\-]+st",
    r"cest\s*[:\s]*\d{2}\.\d{3}\.\d{2}", r"protocolo\s+icms",
    r"conv[eê]nio\s+icms", r"mva|margem\s+de\s+valor", r"cfop\s+5405|cfop\s+6404",
]
SEGMENTOS = {
    "Autopeças":               ["autopeça", "autopeca", "amortecedor", "freio"],
    "Combustíveis":            ["combustível", "gasolina", "diesel", "glp", "lubrificante"],
    "Medicamentos":            ["medicamento", "farmacê", "princípio ativo", "genérico"],
    "Produtos Alimentícios":   ["aliment", "bebida", "laticín", "chocolate", "farinha", "carne"],
    "Perfumaria e Higiene":    ["perfum", "cosmétic", "xampu", "higiene", "sabão", "desodorante"],
    "Eletrônicos":             ["eletrônico", "smartphone", "televisor", "computador"],
    "Materiais de Construção": ["construção", "cimento", "tijolo", "cerâmica", "vergalhão"],
    "Materiais de Limpeza":    ["limpeza", "detergente", "sanitizante", "alvejante"],
    "Pneumáticos":             ["pneumático", "pneu", "câmara de ar"],
    "Tintas e Vernizes":       ["tinta", "verniz", "resina", "esmalte"],
    "Bebidas Alcoólicas":      ["cerveja", "vinho", "aguardente", "cachaça"],
    "Cigarros e Tabaco":       ["cigarro", "tabaco", "fumo", "charuto"],
    "Veículos":                ["veículo automotor", "automóvel", "caminhão", "motocicleta"],
    "Rações Pet":              ["ração", "pet", "animal doméstico"],
}

_GENERATED_COLS_MONO: frozenset = frozenset({})

# Campos que foram removidos da produtos_monofasicos em v3.8
_REMOVED_COLS_MONO: frozenset = frozenset({
    "aliq_pis_simples", "aliq_cofins_simples",
    "aliq_pis_presumido", "aliq_cofins_presumido",
    "aliq_pis_real", "aliq_cofins_real",
    "regime_tributario",
    "_motor_extra",  # campo interno — nunca vai para o banco
})


def _filtrar_payload_mono(registro: dict) -> dict:
    """
    Filtra o payload antes do upsert em produtos_monofasicos.
    Remove campos gerados pelo banco, campos excluídos em v3.8
    e o campo interno _motor_extra.
    """
    excluir = _GENERATED_COLS_MONO | _REMOVED_COLS_MONO
    return {k: v for k, v in registro.items() if k not in excluir}


def limpar_ncm(r):
    d = re.sub(r'[^\d]', '', r)
    return d if 4 <= len(d) <= 8 else None

def nd(raw):
    if not raw: return None
    for f in ('%d/%m/%Y', '%d.%m.%Y', '%d-%m-%Y', '%Y-%m-%d', '%Y/%m/%d'):
        try: return datetime.strptime(raw.strip(), f).strftime('%Y-%m-%d')
        except: pass
    return None

def detectar_regime(texto, folder=""):
    fl = folder.lower()
    if re.search(r'\bmono[_\s]|monofas', fl): return "monofasico", 1.0
    if re.search(r'\bicms[_\s]*st[_\s]|^st[_\s]', fl): return "icms_st", 1.0
    tl = texto.lower()
    sm = sum(1 for p in PADROES_MONO if re.search(p, tl))
    ss = sum(1 for p in PADROES_ST if re.search(p, tl))
    t = sm + ss or 1
    return ("monofasico", round(sm/t, 2)) if sm > ss else ("icms_st", round(ss/t, 2))

def detectar_segmento(texto):
    tl = texto[:5000].lower()
    for seg, pals in SEGMENTOS.items():
        if any(p in tl for p in pals): return seg
    return "Não identificado"

def fund(texto):
    convs = RE_CONV.findall(texto); prots = RE_PROT.findall(texto)
    leis  = RE_LEI.findall(texto);  decs  = RE_DECRETO.findall(texto)
    convenio = None; ricms = None
    if convs: convenio = "Convênio ICMS " + ", ".join(convs[:3])
    if prots: convenio = (convenio + " | " if convenio else "") + "Protocolo ICMS " + ", ".join(prots[:2])
    if leis:  ricms = "Lei " + ", ".join(leis[:2])
    if decs:  ricms = (ricms + " | " if ricms else "") + "Decreto " + ", ".join(decs[:2])
    return {"convenio": convenio, "ricms": ricms}

def now():
    return datetime.utcnow().isoformat()


def mk_mono(ncm, descricao, segmento, cst_pis, natureza, base_legal, lei,
            vigencia_inicio, vigencia_fim, fonte, metodo):
    """
    Monta o payload para produtos_monofasicos (cadastro base puro).
    Não inclui alíquotas nem regime_tributario — esses campos foram
    movidos para as tabelas do motor tributário (v3.8).
    """
    cst = cst_pis or "04"
    vig_ini = vigencia_inicio or "1998-01-01"
    return {
        "ncm":              ncm,
        "descricao":        descricao,
        "segmento":         segmento,
        "cst_pis":          cst,
        "cst_cofins":       cst,
        "descricao_cst":    CST_DESC.get(cst),
        "natureza_receita": natureza,
        "base_legal":       base_legal,
        "lei":              lei,
        "norma_legal_id":   None,
        "normas_relacionadas": None,
        "vigencia_inicio":  vig_ini,
        "vigencia_fim":     vigencia_fim,
        "ativo":            vigencia_fim is None,
        "content":          None,
        "metadata":         None,
        "fonte_arquivo":    fonte,
        "metodo_extracao":  metodo,
        "updated_at":       now(),
    }

def mk_st(ncm, cest, descricao, segmento, cst, csosn,
          cfop_interno, cfop_interestadual,
          fund_conv, fund_ricms,
          vigencia_inicio, vigencia_fim, fonte, metodo):
    return {
        "ncm":                    ncm,
        "cest":                   cest,
        "descricao":              descricao,
        "segmento":               segmento,
        "cst_icms":               cst or "060",
        "csosn":                  csosn or "500",
        "cst":                    cst or "060",
        "cfop_interno":           cfop_interno or "5405",
        "cfop_externo":           cfop_interestadual or "6404",
        "cfop_interestadual":     cfop_interestadual or "6404",
        "fundamentacao_convenio": fund_conv,
        "fundamentacao_ricms":    fund_ricms,
        "vigencia_inicio":        vigencia_inicio,
        "vigencia_fim":           vigencia_fim,
        "ativo":                  vigencia_fim is None,
        "descricao_cst":          CST_DESC.get(cst or "060") or CST_DESC.get("060"),
        "fonte_arquivo":          fonte,
        "metodo_extracao":        metodo,
        "updated_at":             now(),
    }


def extrair_tabela_pdf(page, regime, segmento, fonte):
    items = []
    for table in (page.extract_tables() or []):
        if not table or len(table) < 2: continue
        hdr = [str(c).lower().strip() if c else "" for c in (table[0] or [])]
        col = {
            "ncm":    next((i for i, h in enumerate(hdr) if "ncm" in h or "sh" in h), None),
            "cest":   next((i for i, h in enumerate(hdr) if "cest" in h), None),
            "desc":   next((i for i, h in enumerate(hdr) if any(w in h for w in ["descri", "produto", "mercad"])), None),
            "cst":    next((i for i, h in enumerate(hdr) if h in ["cst", "cst_icms", "cst_pis"]), None),
            "cfop_i": next((i for i, h in enumerate(hdr) if "cfop_i" in h or "interno" in h), None),
            "cfop_e": next((i for i, h in enumerate(hdr) if "cfop_e" in h or "interes" in h), None),
            "vig":    next((i for i, h in enumerate(hdr) if any(w in h for w in ["vigên", "inicio", "data_ini"])), None),
            "rev":    next((i for i, h in enumerate(hdr) if any(w in h for w in ["revog", "vigência_fim", "data_fim"])), None),
            "nat":    next((i for i, h in enumerate(hdr) if "natureza" in h), None),
            "lei":    next((i for i, h in enumerate(hdr) if "lei" in h), None),
        }
        if col["ncm"] is None:
            cts = [sum(1 for r in table[1:] if r and ci < len(r) and r[ci] and RE_NCM.search(str(r[ci])))
                   for ci in range(len(hdr))]
            if cts and max(cts) > 0: col["ncm"] = cts.index(max(cts))
        if col["ncm"] is None: continue
        for row in table[1:]:
            if not row or all(not c for c in row): continue
            def cel(k):
                idx = col.get(k)
                return str(row[idx]).strip() if idx is not None and idx < len(row) and row[idx] else ""
            linha = " ".join(str(c) for c in row if c)
            for nr in (RE_NCM.findall(cel("ncm")) or RE_NCM.findall(linha)):
                ncm = limpar_ncm(nr)
                if not ncm: continue
                cest_m = RE_CEST.search(cel("cest")) or RE_CEST.search(linha)
                vig_m  = RE_VIGENCIA.search(cel("vig") or linha)
                rev_m  = RE_REVOGACAO.search(cel("rev") or linha)
                f      = fund(linha)
                nat_m  = RE_NAT.search(cel("nat"))
                if regime == "monofasico":
                    items.append(mk_mono(ncm, cel("desc") or None, segmento,
                        cel("cst") or None, nat_m.group(1) if nat_m else None,
                        f["ricms"] or f["convenio"], cel("lei") or None,
                        nd(vig_m.group(1)) if vig_m else None,
                        nd(rev_m.group(1)) if rev_m else None, fonte, "tabela"))
                else:
                    cfop_m = RE_CFOP.search(linha)
                    items.append(mk_st(ncm, cest_m.group(1) if cest_m else None,
                        cel("desc") or None, segmento, cel("cst") or None, None,
                        cel("cfop_i") or (cfop_m.group(1) if cfop_m else "5405"),
                        cel("cfop_e") or "6404",
                        f["convenio"], f["ricms"],
                        nd(vig_m.group(1)) if vig_m else None,
                        nd(rev_m.group(1)) if rev_m else None, fonte, "tabela"))
    return items

def extrair_xlsx(conteudo, regime, fonte):
    if not HAS_XLSX or not HAS_PANDAS: return []
    items = []
    try:
        for _, df in pd.read_excel(io.BytesIO(conteudo), sheet_name=None).items():
            df.columns = [str(c).lower().strip() for c in df.columns]
            col_ncm = next((c for c in df.columns if "ncm" in c), None)
            if not col_ncm: continue
            for _, row in df.iterrows():
                ncm = limpar_ncm(str(row.get(col_ncm, "")))
                if not ncm: continue
                def g(keys):
                    for k in ([keys] if isinstance(keys, str) else keys):
                        v = next((row[c] for c in df.columns if k in c), None)
                        if v is not None and pd.notna(v): return str(v).strip()
                    return None
                if regime == "monofasico":
                    items.append(mk_mono(ncm, g("descri"), g("segmento"), g("cst"), g("natureza"),
                        g("base_legal") or g("lei"), g("lei"),
                        nd(g("vigencia_inicio")), nd(g("vigencia_fim")), fonte, "tabela_xlsx"))
                else:
                    items.append(mk_st(ncm, g("cest"), g("descri"), g("segmento"), g("cst"), g("csosn"),
                        g(["cfop_interno", "cfop_i"]), g(["cfop_interestadual", "cfop_e"]),
                        g("fundamentacao_convenio"), g("fundamentacao_ricms"),
                        nd(g("vigencia_inicio")), nd(g("vigencia_fim")), fonte, "tabela_xlsx"))
    except Exception as e: log.error(f"XLSX: {e}")
    return items

def extrair_csv(conteudo, regime, fonte):
    if not HAS_PANDAS: return []
    try:
        for enc in ("utf-8", "latin-1", "cp1252"):
            try: df = pd.read_csv(io.BytesIO(conteudo), encoding=enc, sep=None, engine="python"); break
            except: pass
        else: return []
        df.columns = [str(c).lower().strip() for c in df.columns]
        col_ncm = next((c for c in df.columns if "ncm" in c), None)
        if not col_ncm: return []
        items = []
        for _, row in df.iterrows():
            ncm = limpar_ncm(str(row.get(col_ncm, "")))
            if not ncm: continue
            if regime == "monofasico":
                items.append(mk_mono(ncm, None, None, "04", None, None, None, None, None, fonte, "csv"))
            else:
                items.append(mk_st(ncm, None, None, None, "060", None, "5405", "6404", None, None, None, None, fonte, "csv"))
        return items
    except Exception as e: log.error(f"CSV: {e}"); return []


def extrair_regex(texto, regime, segmento, fonte):
    items = []
    linhas = texto.split("\n")
    f = fund(texto)
    for i, linha in enumerate(linhas):
        for nr in RE_NCM.findall(linha):
            ncm = limpar_ncm(nr)
            if not ncm: continue
            ctx    = " ".join(linhas[max(0, i-2):i+3])
            cest_m = RE_CEST.search(ctx)
            partes = linha.split(nr, 1)
            desc   = partes[1].strip()[:300] if len(partes) > 1 else None
            vig_m  = RE_VIGENCIA.search(ctx)
            rev_m  = RE_REVOGACAO.search(ctx)
            nat_m  = RE_NAT.search(ctx)
            if regime == "monofasico":
                items.append(mk_mono(ncm, desc, segmento, "04",
                    nat_m.group(1) if nat_m else None,
                    f["ricms"] or f["convenio"], None,
                    nd(vig_m.group(1)) if vig_m else None,
                    nd(rev_m.group(1)) if rev_m else None, fonte, "regex"))
            else:
                cfop_m = RE_CFOP.search(ctx)
                items.append(mk_st(ncm, cest_m.group(1) if cest_m else None, desc, segmento,
                    "060", None, cfop_m.group(1) if cfop_m else "5405", "6404",
                    f["convenio"], f["ricms"],
                    nd(vig_m.group(1)) if vig_m else None,
                    nd(rev_m.group(1)) if rev_m else None, fonte, "regex"))
    return items


async def extrair_llm(texto, regime, fonte):
    if not OPENAI_API_KEY or not HAS_HTTPX: return []
    s_mono = '{"ncm":"XXXXXXXX","descricao":"...","cst_pis":"04","natureza_receita":"XXX ou null","base_legal":"Lei ... ou null","segmento":"..."}'
    s_st   = '{"ncm":"XXXXXXXX","cest":"XX.XXX.XX ou null","descricao":"...","segmento":"...","fundamentacao_convenio":"Conv. ICMS ... ou null"}'
    schema = s_mono if regime == "monofasico" else s_st
    label  = "PIS/Cofins monofásico" if regime == "monofasico" else "ICMS Substituição Tributária"
    try:
        async with httpx.AsyncClient(timeout=90) as c:
            r = await c.post("https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
                json={"model": "gpt-4o-mini", "max_tokens": 4000, "messages": [
                    {"role": "system", "content": "Extrator fiscal. Retorne apenas JSON array válido."},
                    {"role": "user",   "content": f"Extraia NCMs de {label}. Array JSON:\n[{schema}]\nTexto:\n{texto[:10000]}"}
                ]})
        raw    = re.sub(r"```json|```", "", r.json()["choices"][0]["message"]["content"].strip()).strip()
        parsed = json.loads(raw)
        if not isinstance(parsed, list): return []
        items = []
        for p in parsed:
            ncm = limpar_ncm(str(p.get("ncm", "")))
            if not ncm: continue
            if regime == "monofasico":
                items.append(mk_mono(ncm, p.get("descricao"), p.get("segmento"),
                    p.get("cst_pis", "04"), p.get("natureza_receita"), p.get("base_legal"),
                    None, None, None, fonte, "llm"))
            else:
                items.append(mk_st(ncm, p.get("cest"), p.get("descricao"), p.get("segmento"),
                    "060", None, "5405", "6404", p.get("fundamentacao_convenio"),
                    None, None, None, fonte, "llm"))
        return items
    except Exception as e: log.error(f"LLM: {e}"); return []


def dedup(items):
    seen = {}
    for item in items:
        k = item["ncm"]
        if k not in seen or sum(1 for v in item.values() if v) > sum(1 for v in seen[k].values() if v):
            seen[k] = item
    return list(seen.values())

def salvar(mono, st):
    if not supabase: return {"mono": 0, "st": 0, "erros": ["Supabase não configurado"]}
    r = {"mono": 0, "st": 0, "erros": []}
    if mono:
        mono_filtrado = [_filtrar_payload_mono(m) for m in mono]
        try:
            supabase.table("produtos_monofasicos").upsert(mono_filtrado, on_conflict="ncm").execute()
            r["mono"] = len(mono_filtrado)
            # ── v3.7: espelho nas tabelas do motor tributário ─────────────────
            # Passa `mono` (original, com _motor_extra) para o motor
            motor = _upsert_motor_tributario(mono, origem="extrair-ncms")
            if motor["erros"]:
                r["erros"].extend(motor["erros"])
        except Exception as e: r["erros"].append(f"mono:{str(e)[:200]}"); log.error(e)
    if st:
        try: supabase.table("produtos_icms_st").upsert(st, on_conflict="ncm").execute(); r["st"] = len(st)
        except Exception as e: r["erros"].append(f"st:{str(e)[:200]}"); log.error(e)
    return r

def log_proc(arquivo, folder, tipo, metodo, n_mono, n_st, avisos, erros, ms):
    if not supabase: return
    try:
        supabase.table("log_processamento").insert({
            "arquivo": arquivo, "folder_name": folder, "tipo_arquivo": tipo,
            "metodo_extracao": metodo, "total_monofasicos": n_mono, "total_icms_st": n_st,
            "avisos": avisos, "erros": erros, "duracao_ms": ms
        }).execute()
    except: pass


async def processar(conteudo, file_name, file_type, folder_name, forcar_regime):
    t0 = datetime.utcnow()
    mono, st, avisos = [], [], []
    txt_total = ""
    metodo = "tabela"
    ext = file_type.lower().strip(".")

    if ext == "xlsx":
        regime, _ = detectar_regime("", folder_name)
        if forcar_regime: regime = forcar_regime
        for item in extrair_xlsx(conteudo, regime, file_name):
            (mono if regime == "monofasico" else st).append(item)
        metodo = "tabela_xlsx"
    elif ext == "csv":
        regime, _ = detectar_regime("", folder_name)
        if forcar_regime: regime = forcar_regime
        for item in extrair_csv(conteudo, regime, file_name):
            (mono if regime == "monofasico" else st).append(item)
        metodo = "csv"
    elif ext == "docx" and HAS_DOCX:
        doc = Docx(io.BytesIO(conteudo))
        txt_total = "\n".join(p.text for p in doc.paragraphs)
        regime, _ = detectar_regime(txt_total, folder_name)
        if forcar_regime: regime = forcar_regime
        seg = detectar_segmento(txt_total)
        for item in extrair_regex(txt_total, regime, seg, file_name):
            (mono if regime == "monofasico" else st).append(item)
        metodo = "regex"
    elif ext == "pdf" and HAS_PDF:
        pags_tab = 0; txts = []
        with pdfplumber.open(io.BytesIO(conteudo)) as pdf:
            log.info(f"PDF '{file_name}' — {len(pdf.pages)} pgs")
            for n, page in enumerate(pdf.pages, 1):
                txt = page.extract_text() or ""
                txts.append(txt)
                regime, _ = detectar_regime(txt, folder_name if n == 1 else "")
                if forcar_regime: regime = forcar_regime
                seg = detectar_segmento(txt)
                tab = extrair_tabela_pdf(page, regime, seg, file_name)
                if tab:
                    pags_tab += 1
                    for item in tab: (mono if regime == "monofasico" else st).append(item)
                else:
                    for item in extrair_regex(txt, regime, seg, file_name):
                        (mono if regime == "monofasico" else st).append(item)
        txt_total = "\n".join(txts)
        metodo = "tabela" if pags_tab > 0 else "regex"
    else:
        avisos.append(f"Formato '{ext}' não suportado ou biblioteca ausente")

    if not mono and not st and txt_total:
        avisos.append("Camadas 1+2 sem NCMs — acionando LLM")
        regime, _ = detectar_regime(txt_total, folder_name)
        if forcar_regime: regime = forcar_regime
        for item in await extrair_llm(txt_total, regime, file_name):
            (mono if regime == "monofasico" else st).append(item)
        metodo = "llm"

    mono = dedup(mono); st = dedup(st)
    if not mono and not st: avisos.append("Nenhum NCM extraído")
    log.info(f"'{file_name}' → {len(mono)} mono | {len(st)} ST | {metodo}")
    ms = int((datetime.utcnow() - t0).total_seconds() * 1000)
    return {"mono": mono, "st": st, "metodo": metodo, "avisos": avisos, "ms": ms}


def _svc():
    if not HAS_GDRIVE: return None
    try:
        import tempfile
        creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
        if creds_json:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                f.write(creds_json)
                tmp_path = f.name
            creds = service_account.Credentials.from_service_account_file(
                tmp_path, scopes=["https://www.googleapis.com/auth/drive.readonly"])
        else:
            creds = service_account.Credentials.from_service_account_file(
                GOOGLE_CREDENTIALS_PATH,
                scopes=["https://www.googleapis.com/auth/drive.readonly"])
        return build("drive", "v3", credentials=creds)
    except Exception as e: log.error(f"Drive auth: {e}"); return None


@app.get("/health")
def health():
    return {
        "status": "ok", "versao": "3.9.0",
        "supabase": supabase is not None,
        "pdf": HAS_PDF, "xlsx": HAS_XLSX, "docx": HAS_DOCX, "gdrive": HAS_GDRIVE,
        "removed_cols_mono": sorted(_REMOVED_COLS_MONO - {"_motor_extra"}),
    }

@app.get("/status")
def status_db():
    if not supabase: return {"erro": "Supabase não configurado"}
    try:
        m  = supabase.table("produtos_monofasicos").select("ncm", count="exact").execute()
        s  = supabase.table("produtos_icms_st").select("ncm", count="exact").execute()
        tc = supabase.table("produtos_tributacao_concentrada").select("ncm", count="exact").execute()
        az = supabase.table("produtos_aliquota_zero").select("ncm", count="exact").execute()
        return {
            "produtos_monofasicos":             m.count,
            "produtos_icms_st":                 s.count,
            "produtos_tributacao_concentrada":  tc.count,
            "produtos_aliquota_zero":           az.count,
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as e: return {"erro": str(e)}

@app.post("/extrair-ncms")
async def extrair_ncms(req: ExtrairRequest):
    if not req.pdf_base64: raise HTTPException(400, "pdf_base64 obrigatório")
    try: conteudo = base64.b64decode(req.pdf_base64)
    except: raise HTTPException(400, "pdf_base64 inválido")
    res    = await processar(conteudo, req.file_name, req.file_type, req.folder_name, req.forcar_regime)
    salvos = salvar(res["mono"], res["st"])
    log_proc(req.file_name, req.folder_name, req.file_type, res["metodo"],
             len(res["mono"]), len(res["st"]), res["avisos"], salvos.get("erros", []), res["ms"])
    return {
        "monofasicos": res["mono"], "icms_st": res["st"],
        "total_monofasicos": len(res["mono"]), "total_icms_st": len(res["st"]),
        "metodo_principal": res["metodo"], "arquivo": req.file_name,
        "avisos": res["avisos"], "erros": salvos.get("erros", []), "duracao_ms": res["ms"],
    }

@app.post("/processar-drive")
async def processar_drive(background_tasks: BackgroundTasks, folder_id: Optional[str] = None):
    fid = folder_id or DRIVE_FOLDER_ID
    if not fid: raise HTTPException(400, "folder_id não informado")
    background_tasks.add_task(_pasta, fid)
    return {"status": "iniciado", "folder_id": fid}

async def _pasta(folder_id):
    svc = _svc()
    if not svc: return
    TIPOS = {
        "application/pdf": "pdf",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
        "text/csv": "csv",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    }
    try:
        arqs = svc.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="files(id,name,mimeType)", pageSize=200
        ).execute().get("files", [])
    except Exception as e: log.error(f"Listar Drive: {e}"); return
    for arq in arqs:
        ext = TIPOS.get(arq["mimeType"])
        if not ext: continue
        try:
            buf = io.BytesIO()
            dl  = MediaIoBaseDownload(buf, svc.files().get_media(fileId=arq["id"]))
            done = False
            while not done: _, done = dl.next_chunk()
            conteudo = buf.getvalue()
        except Exception as e: log.error(f"Baixar {arq['name']}: {e}"); continue
        res    = await processar(conteudo, arq["name"], ext, arq["name"], None)
        salvos = salvar(res["mono"], res["st"])
        log_proc(arq["name"], folder_id, ext, res["metodo"],
                 len(res["mono"]), len(res["st"]), res["avisos"], salvos.get("erros", []), res["ms"])


# =============================================================================
# UPSERT JSONs ICMS-ST  v3.4
# =============================================================================

_TABLE_ORDER = [
    "normas_legais",
    "anexos_fiscais",
    "anexos_artigos",
    "anexos_pontos_atencao",
    "anexos_comparativo_redacao",
    "anexos_normas",
    "produtos_icms_st",
]

_SCHEMA_COLS: dict = {
    "normas_legais": {
        "tipo_norma", "numero", "ano", "ementa", "data_publicacao",
        "orgao_emissor", "esfera", "texto_completo", "url_fonte", "tags",
        "uf", "artigo", "status", "data_vigencia", "data_revogacao",
        "revogado_por", "versao_atual", "ultima_atualizacao",
    },
    "anexos_fiscais": {
        "estado", "orgao", "nome_completo", "sigla_anexo", "regulamento",
        "produto_principal", "data_inicio_vigencia_estado", "ultima_alteracao_norma",
        "data_inicio_ultima_alteracao", "signatarios_ativos", "ex_signatarios", "ementa",
    },
    "anexos_artigos": {
        "anexo_id", "numero", "secao", "epigrafe", "resumo",
        "texto_chave", "paragrafos_incisos",
    },
    "anexos_pontos_atencao": {"anexo_id", "descricao", "tema"},
    "anexos_comparativo_redacao": {
        "anexo_id", "artigo", "dispositivo", "redacao_anterior",
        "redacao_vigente", "data_mudanca", "norma_que_alterou",
    },
    "anexos_normas": {
        "anexo_id", "norma_id", "papel",
        "anexo_fiscal_id", "norma_legal_id", "tipo_relacao",  # aliases aceitos
    },
    "produtos_icms_st": {
        "segmento", "cest", "ncm", "descricao", "fundamentacao_convenio",
        "fundamentacao_ricms", "cst_icms", "csosn", "cfop_interno", "cfop_externo",
        "cst", "cfop_interestadual", "vigencia_inicio", "vigencia_fim", "ativo",
        "norma_legal_id", "descricao_cst", "fonte_arquivo", "metodo_extracao",
        "anexo_fiscal_id",
    },
}

_FIELD_ALIASES: dict = {
    "normas_legais": {
        "tipo":                 "tipo_norma",
        "orgao":                "orgao_emissor",
        "texto_ementa":         "ementa",
        "descricao":            "ementa",
        "data_vigencia_inicio": "data_vigencia",
        "data_vigencia_fim":    "data_revogacao",
        "situacao":             "status",
        "revogada":             "status",
    },
    "anexos_normas": {
        "anexo_fiscal_id": "anexo_id",
        "norma_legal_id":  "norma_id",
        "tipo_relacao":    "papel",
    },
}

_IGNORE_FIELDS = frozenset({
    "ato", "dispositivos_afetados", "dispositivos_impactados", "ncm_produto",
    "ambito",
})

_DEFAULTS: dict = {
    "normas_legais": {
        "tipo_norma": "Norma",
        "numero":     "0",
        "ano":        0,
    },
    "anexos_fiscais": {
        "estado":      "MA",
        "orgao":       "SEFAZ-MA",
        "regulamento": "RICMS/MA — Decreto nº 19.714/2003",
    },
    "produtos_icms_st": {
        "segmento":           "Não classificado",
        "cest":               "00.000.00",
        "ncm":                "0000",
        "descricao":          "Sem descrição",
        "cst_icms":           "60",
        "csosn":              "500",
        "cfop_interno":       "5405",
        "cfop_interestadual": "6403",
        "ativo":              True,
        "metodo_extracao":    "manual",
    },
}

_PH_RE = re.compile(r"^<[A-Za-z0-9_\-]+>$")

def _is_placeholder(v) -> bool:
    return isinstance(v, str) and bool(_PH_RE.match(v))

def _is_blank(v) -> bool:
    if v is None: return True
    if isinstance(v, str) and (v.strip() == "" or _is_placeholder(v)): return True
    return False

_SENTINEL_FAILED = "__FAILED__"


class _Resolver:
    def __init__(self, filename: str = ""):
        self._map: dict = {}
        self._file = filename

    def set_file(self, filename: str):
        self._file = filename

    def register(self, placeholder: str, real_id: str):
        if placeholder and real_id:
            self._map[placeholder] = real_id
            log.info(f"[icms] {self._file} — registrado {placeholder} = {real_id[:8]}…")

    def register_failed(self, placeholder: str):
        if placeholder:
            self._map[placeholder] = _SENTINEL_FAILED
            log.warning(
                f"[icms] {self._file} — placeholder marcado como FAILED: {placeholder} "
                f"(insert da norma falhou; registros dependentes serão ignorados)"
            )

    def resolve(self, value):
        if _is_placeholder(value):
            resolved = self._map.get(value)
            if resolved is None:
                log.warning(f"[icms] {self._file} — placeholder não resolvido: {value}")
                return None
            if resolved == _SENTINEL_FAILED:
                return None
            return resolved
        return value

    def resolve_all(self, record: dict) -> dict:
        return {k: self.resolve(v) for k, v in record.items()}


class _GlobalResolver(_Resolver):
    """Resolver compartilhado entre todos os arquivos de uma execução."""
    pass


def _normalizar_registro(table: str, raw: dict, filename: str) -> dict:
    result = {}
    aliases = _FIELD_ALIASES.get(table, {})
    schema  = _SCHEMA_COLS.get(table, set())

    for key, val in raw.items():
        if key.startswith("_"):
            continue
        if key in _IGNORE_FIELDS:
            log.debug(f"[icms] {filename}/{table}: campo ignorado '{key}'")
            continue
        target_key = aliases.get(key, key)
        if target_key != key:
            if key == "revogada" and target_key == "status":
                val = "REVOGADO" if val else "ATIVO"
            if target_key in result:
                log.warning(
                    f"[icms] {filename}/{table}: campo duplicado — "
                    f"'{key}' é alias de '{target_key}' que já existe. "
                    f"Mantendo '{target_key}' = {repr(result[target_key])!r:.60s}, "
                    f"ignorando '{key}' = {repr(val)!r:.60s}"
                )
                continue
            key = target_key
        if key not in schema:
            log.warning(f"[icms] {filename}/{table}: campo '{key}' fora do schema — ignorado")
            continue
        result[key] = val

    for field, default_val in _DEFAULTS.get(table, {}).items():
        if _is_blank(result.get(field)):
            result[field] = default_val

    return result


def _extrair_rows(section) -> list:
    if not section:
        return []
    if isinstance(section, list):
        return [r for r in section if isinstance(r, dict)]
    if isinstance(section, dict):
        if "registros" in section:
            regs = section["registros"]
            return [r for r in regs if isinstance(r, dict)] if isinstance(regs, list) else []
        if any(k in section for k in _SCHEMA_COLS.get("anexos_fiscais", set())):
            return [section]
    return []


_UUID_FK_FIELDS = frozenset({
    "anexo_id", "norma_id", "anexo_fiscal_id", "norma_legal_id",
})

def _sanitize_unresolved(row: dict, filename: str, table: str) -> dict:
    result = {}
    for k, v in row.items():
        if _is_placeholder(v):
            log.warning(
                f"[icms] {filename}/{table}: campo '{k}' ainda é placeholder "
                f"após resolução: {v} — setado como None"
            )
            result[k] = None
        elif k in _UUID_FK_FIELDS and isinstance(v, str) and not v.strip():
            result[k] = None
        else:
            result[k] = v
    return result


_NOT_NULL_COLS: dict = {
    "normas_legais":   ["tipo_norma", "numero", "ano"],
    "anexos_fiscais":  ["estado", "nome_completo"],
    "anexos_artigos":  ["anexo_id", "numero"],
    "anexos_pontos_atencao":      ["anexo_id", "descricao"],
    "anexos_comparativo_redacao": ["anexo_id", "artigo"],
    "anexos_normas":              ["anexo_id", "norma_id"],
    "produtos_icms_st":           ["segmento", "ncm", "cest", "descricao", "ativo"],
}

_UPSERT_CONFLICT: dict = {
    "normas_legais":    "tipo_norma,numero,ano",
    "anexos_fiscais":   "sigla_anexo,estado",
    "produtos_icms_st": "cest,ncm,anexo_fiscal_id",
}


def _expandir_ncms(row: dict) -> list:
    ncm_raw = row.get("ncm") or ""
    if ";" in str(ncm_raw):
        separador = ";"
    elif re.search(r"\d,\s*\d{4}", str(ncm_raw)):
        separador = ","
    else:
        return [row]
    tokens = [t.strip() for t in str(ncm_raw).split(separador)]
    tokens = [t for t in tokens if t]
    if len(tokens) <= 1:
        return [row]
    rows = []
    for ncm in tokens:
        novo = dict(row)
        novo["ncm"] = ncm
        rows.append(novo)
    return rows


def _processar_bloco(bloco_data: dict, filename: str, resolver: _Resolver) -> dict:
    contadores = {t: 0 for t in _TABLE_ORDER}
    erros: list = []

    for table in _TABLE_ORDER:
        rows = _extrair_rows(bloco_data.get(table))
        if not rows:
            continue

        if table == "produtos_icms_st":
            rows_expandidas = []
            for r in rows:
                rows_expandidas.extend(_expandir_ncms(r))
            if len(rows_expandidas) != len(rows):
                log.info(
                    f"[icms] {filename}/{table}: {len(rows)} registro(s) "
                    f"expandidos para {len(rows_expandidas)} (NCMs múltiplos)"
                )
                rows = rows_expandidas

        log.info(f"[icms] {filename}/{table}: {len(rows)} registro(s)")

        for row in rows:
            placeholder = row.get("_id_referencia")
            row_norm = resolver.resolve_all(dict(row))
            row_norm = _sanitize_unresolved(row_norm, filename, table)
            row_norm = _normalizar_registro(table, row_norm, filename)

            if table == "anexos_normas":
                row_norm.pop("anexo_fiscal_id", None)
                row_norm.pop("norma_legal_id", None)
                row_norm.pop("tipo_relacao", None)

            skip = False
            for col in _NOT_NULL_COLS.get(table, []):
                if row_norm.get(col) is None:
                    log.warning(
                        f"[icms] {filename}/{table}: campo NOT NULL '{col}' é None "
                        f"— registro ignorado: {list(row_norm.items())[:3]}"
                    )
                    skip = True
                    break
            if skip:
                if table == "normas_legais" and placeholder:
                    resolver.register_failed(placeholder)
                ref = placeholder or str(list(row_norm.items())[:2])
                erros.append(f"{table}:{ref}:null_not_null")
                continue

            real_id = None
            insert_ok = False
            conflict_cols = _UPSERT_CONFLICT.get(table)

            try:
                if table == "normas_legais" and conflict_cols:
                    try:
                        resp = supabase.table(table).insert(row_norm).execute()
                        resp_data = resp.data
                        real_id = resp_data[0].get("id") if resp_data else None
                    except Exception as e_insert:
                        if "23505" in str(e_insert) or "unique" in str(e_insert).lower():
                            filter_cols = conflict_cols.split(",")
                            q = supabase.table(table).select("id")
                            for col in filter_cols:
                                val = row_norm.get(col)
                                if val is not None:
                                    q = q.eq(col, val)
                            fallback = q.limit(1).execute()
                            if fallback.data:
                                real_id = fallback.data[0].get("id")
                                log.info(
                                    f"[icms] {filename}/{table}: norma já existe "
                                    f"— UUID recuperado via SELECT: {real_id[:8]}…"
                                )
                        else:
                            raise e_insert

                elif conflict_cols:
                    resp = (
                        supabase.table(table)
                        .upsert(row_norm, on_conflict=conflict_cols)
                        .execute()
                    )
                    resp_data = resp.data
                    real_id = resp_data[0].get("id") if resp_data else None

                    if real_id is None:
                        filter_cols = conflict_cols.split(",")
                        q = supabase.table(table).select("id")
                        for col in filter_cols:
                            val = row_norm.get(col)
                            if val is not None:
                                q = q.eq(col, val)
                        fallback = q.limit(1).execute()
                        if fallback.data:
                            real_id = fallback.data[0].get("id")
                            log.info(
                                f"[icms] {filename}/{table}: UUID recuperado via "
                                f"SELECT fallback — {real_id[:8]}…"
                            )
                else:
                    resp = supabase.table(table).insert(row_norm).execute()
                    resp_data = resp.data
                    real_id = resp_data[0].get("id") if resp_data else None

                insert_ok = real_id is not None

            except Exception as e:
                log.error(
                    f"[icms] {filename}/{table}: insert erro — {e} "
                    f"| dados: {list(row_norm.items())}"
                )

            if insert_ok:
                if placeholder:
                    resolver.register(placeholder, real_id)
                contadores[table] += 1
            else:
                if placeholder:
                    resolver.register_failed(placeholder)
                ref = (
                    placeholder
                    or row_norm.get("ncm")
                    or row_norm.get("sigla_anexo")
                    or str(row_norm.get("descricao", ""))[:40]
                    or "?"
                )
                erros.append(f"{table}:{ref}")

    return {"contadores": contadores, "erros": erros}


def _normalizar_blocos(data) -> list:
    if isinstance(data, list):
        return [b for b in data if isinstance(b, dict)]
    if isinstance(data, dict):
        if any(k in data for k in _TABLE_ORDER):
            return [data]
        blocos = [
            block for key, block in data.items()
            if isinstance(block, dict) and any(t in block for t in _TABLE_ORDER)
        ]
        return blocos
    return []


async def _run_upsert_icms_jsons(folder_id: str):
    svc = _svc()
    if not svc:
        log.error("[icms] Falha na autenticação Google Drive")
        return

    try:
        files = (
            svc.files()
            .list(
                q=(
                    f"'{folder_id}' in parents "
                    f"and mimeType='application/json' "
                    f"and trashed=false"
                ),
                fields="files(id, name)",
                pageSize=100,
            )
            .execute()
            .get("files", [])
        )
    except Exception as e:
        log.error(f"[icms] Listar pasta Drive: {e}")
        return

    log.info(f"[icms] {len(files)} arquivo(s) JSON na pasta {folder_id}")

    total  = {t: 0 for t in _TABLE_ORDER}
    erros_geral: list = []

    resolver = _GlobalResolver()

    for file_meta in files:
        file_id  = file_meta["id"]
        filename = file_meta["name"]
        log.info(f"[icms] baixando '{filename}'…")

        try:
            buf = io.BytesIO()
            dl  = MediaIoBaseDownload(buf, svc.files().get_media(fileId=file_id))
            done = False
            while not done:
                _, done = dl.next_chunk()
            bloco_json = json.loads(buf.getvalue().decode("utf-8"))
        except Exception as e:
            log.error(f"[icms] Falha ao baixar '{filename}': {e}")
            erros_geral.append(f"download:{filename}")
            continue

        resolver.set_file(filename)
        blocos = _normalizar_blocos(bloco_json)

        if not blocos:
            log.warning(f"[icms] '{filename}': nenhum bloco reconhecível — ignorado")
            continue

        for bloco in blocos:
            resultado = _processar_bloco(bloco, filename, resolver)
            for t, n in resultado["contadores"].items():
                total[t] += n
            erros_geral.extend(resultado["erros"])

    log.info(f"[icms] ✓ CONCLUÍDO — totais: {total}")
    if erros_geral:
        log.warning(f"[icms] {len(erros_geral)} falha(s): {erros_geral[:30]}")


# =============================================================================
# PIPELINE MONOFÁSICOS v3.9
# =============================================================================

_SCHEMA_MONO = {
    "ncm", "descricao", "segmento",
    "cst_pis", "cst_cofins", "descricao_cst", "natureza_receita",
    "base_legal", "lei", "norma_legal_id", "normas_relacionadas",
    "vigencia_inicio", "vigencia_fim", "ativo",
    "content", "metadata",
    "fonte_arquivo", "metodo_extracao", "updated_at",
}

_SEGMENTO_GRUPO: list = [
    ("combustív",           "Combustíveis"),
    ("derivados de petról", "Combustíveis"),
    ("petróleo",            "Combustíveis"),
    ("biodiesel",           "Combustíveis"),
    ("energia elétr",       "Combustíveis"),
    ("farmacê",             "Medicamentos"),
    ("farmaci",             "Medicamentos"),
    ("fármacos",            "Medicamentos"),
    ("medicament",          "Medicamentos"),
    ("produtos farmac",     "Medicamentos"),
    ("higiene",             "Perfumaria e Higiene"),
    ("cosmétic",            "Perfumaria e Higiene"),
    ("perfumaria",          "Perfumaria e Higiene"),
    ("cosméticos",          "Perfumaria e Higiene"),
    ("veículos automotor",  "Veículos"),
    ("automotor",           "Veículos"),
    ("veículos",            "Veículos"),
    ("motocicleta",         "Veículos"),
    ("bebida",              "Bebidas Alcoólicas"),
    ("cerveja",             "Bebidas Alcoólicas"),
    ("vinho",               "Bebidas Alcoólicas"),
    ("suco",                "Bebidas Alcoólicas"),
    ("bebidas frias",       "Bebidas Alcoólicas"),
    ("embalagem",           "Bebidas Alcoólicas"),
    ("máquina",             "Materiais de Construção"),
    ("equipament",          "Materiais de Construção"),
    ("construção",          "Materiais de Construção"),
    ("agrícola",            "Materiais de Construção"),
    ("hortícola",           "Materiais de Construção"),
    ("cesta básica",        "Produtos Alimentícios"),
    ("aliment",             "Produtos Alimentícios"),
    ("laticín",             "Produtos Alimentícios"),
    ("farinha",             "Produtos Alimentícios"),
    ("carne",               "Produtos Alimentícios"),
    ("leite",               "Produtos Alimentícios"),
    ("açúcar",              "Produtos Alimentícios"),
    ("arroz",               "Produtos Alimentícios"),
    ("trigo",               "Produtos Alimentícios"),
    ("óleo",                "Produtos Alimentícios"),
    ("saúde",               "Saúde"),
    ("ortopéd",             "Saúde"),
    ("prótese",             "Saúde"),
    ("cadeira de roda",     "Saúde"),
    ("portadores de defici","Saúde"),
    ("audição",             "Saúde"),
    ("lentes",              "Saúde"),
    ("aeronave",            "Aeronaves e Embarcações"),
    ("helicóptero",         "Aeronaves e Embarcações"),
    ("avião",               "Aeronaves e Embarcações"),
    ("embarcação",          "Aeronaves e Embarcações"),
    ("navio",               "Aeronaves e Embarcações"),
    ("barco",               "Aeronaves e Embarcações"),
]


def _segmento_por_grupo(group_label: str) -> str:
    gl = group_label.lower()
    for fragmento, segmento in _SEGMENTO_GRUPO:
        if fragmento in gl:
            return segmento
    return "Não identificado"


def _parse_aliq(valor):
    if not valor or str(valor).strip() in ("var.", "–", "-", ""):
        return None
    try:
        return float(str(valor).replace("%", "").replace(",", ".").strip())
    except (ValueError, TypeError):
        return None


def _mapear_item_monofasico(item: dict, group_label: str, filename: str):
    ncm_raw = str(item.get("ncm") or "").strip()
    ncm     = limpar_ncm(ncm_raw)
    if not ncm:
        log.warning(f"[mono] {filename}: NCM inválido '{ncm_raw}' — item ignorado")
        return None

    descricao = (
        str(item.get("descricao") or
            item.get("description") or
            item.get("produto") or
            item.get("mercadoria") or "").strip()
        or "Sem descrição"
    )

    cst_raw = str(
        item.get("cst_pis") or
        item.get("cst_mono_04") or
        item.get("cst") or
        item.get("cst_cofins") or
        "04"
    ).strip()
    if cst_raw.isdigit() and len(cst_raw) > 2 and int(cst_raw) < 100:
        cst_raw = str(int(cst_raw)).zfill(2)

    legal_basis = (
        str(item.get("base_legal") or
            item.get("legal_basis") or
            item.get("fundamento_legal") or
            item.get("fundamentacao") or "").strip()
        or None
    )

    cod_nat = str(item.get("cod_nat_rec") or item.get("natureza_receita") or "").strip() or None

    def _aliq(campo_sped, campo_alt, fallback):
        v = _parse_aliq(item.get(campo_sped)) or _parse_aliq(item.get(campo_alt))
        if v is not None:
            return v
        raw = str(item.get(campo_sped) or item.get(campo_alt) or "").strip()
        if raw.lower() in ("var.", "var", "–", "-", ""):
            return None
        return fallback

    if cst_raw == "06":
        aliq_pis_pres  = 0.0
        aliq_cof_pres  = 0.0
        aliq_pis_real  = 0.0
        aliq_cof_real  = 0.0
    elif cst_raw == "01":
        aliq_pis_pres  = _aliq("al_pis_maj_percent",  "aliq_pis_presumido",  0.65)
        aliq_cof_pres  = _aliq("al_cof_maj_percent",  "aliq_cofins_presumido", 3.0)
        aliq_pis_real  = _aliq("al_pis_norm_percent", "aliq_pis_real",        1.65)
        aliq_cof_real  = _aliq("al_cof_norm_percent", "aliq_cofins_real",     7.6)
    else:
        aliq_pis_pres  = _aliq("al_pis_maj_percent",  "aliq_pis_presumido",  0.65)
        aliq_cof_pres  = _aliq("al_cof_maj_percent",  "aliq_cofins_presumido", 3.0)
        aliq_pis_real  = _aliq("al_pis_norm_percent", "aliq_pis_real",        1.65)
        aliq_cof_real  = _aliq("al_cof_norm_percent", "aliq_cofins_real",     7.6)

    lei = None
    if legal_basis:
        lei_match = RE_LEI.search(legal_basis)
        lei = "Lei " + lei_match.group(1) if lei_match else None

    segmento   = _segmento_por_grupo(group_label)
    vig_inicio = nd(str(item.get("vigencia_inicio") or "").strip()) or "1998-01-01"
    vig_fim    = nd(str(item.get("vigencia_fim") or "").strip())
    regime_trib = "aliquota_zero" if cst_raw == "06" else "monofasico"

    payload_base = {
        "ncm":                 ncm,
        "descricao":           descricao,
        "segmento":            segmento,
        "cst_pis":             cst_raw,
        "cst_cofins":          cst_raw,
        "descricao_cst":       CST_DESC.get(cst_raw),
        "natureza_receita":    cod_nat,
        "base_legal":          legal_basis,
        "lei":                 lei,
        "norma_legal_id":      None,
        "normas_relacionadas": None,
        "vigencia_inicio":     vig_inicio,
        "vigencia_fim":        vig_fim,
        "ativo":               vig_fim is None,
        "content":             None,
        "metadata":            None,
        "fonte_arquivo":       filename,
        "metodo_extracao":     "json_estruturado",
        "updated_at":          now(),
    }

    # ── _motor_extra: alíquotas e regime para uso EXCLUSIVO pelo motor ────────
    # Nunca gravado em produtos_monofasicos — removido por _filtrar_payload_mono
    payload_base["_motor_extra"] = {
        "aliq_pis_simples":      0,
        "aliq_cofins_simples":   0,
        "aliq_pis_presumido":    aliq_pis_pres,
        "aliq_cofins_presumido": aliq_cof_pres,
        "aliq_pis_real":         aliq_pis_real,
        "aliq_cofins_real":      aliq_cof_real,
        "regime_tributario":     regime_trib,
        "fundamentacao_resumida": (
            item.get("fundamentacao_resumida") or
            _FUNDAMENTACAO_LOOKUP.get(ncm) or
            _FUNDAMENTACAO_LOOKUP.get(ncm[:4])
        ),
    }

    return payload_base


# =============================================================================
# MOTOR TRIBUTÁRIO — espelho paralelo em produtos_tributacao_concentrada
#                    e produtos_aliquota_zero (v3.7)
# =============================================================================

_SCHEMA_TRIB_CONC = {
    "ncm", "descricao", "segmento", "cst_pis", "cst_cofins",
    "descricao_cst", "natureza_receita", "regime_tributario",
    "aliq_pis_simples", "aliq_cofins_simples",
    "aliq_pis_presumido", "aliq_cofins_presumido",
    "aliq_pis_real", "aliq_cofins_real",
    "aliquota_por_pauta",
    "base_legal", "lei", "fundamentacao_resumida",
    "norma_legal_id", "normas_relacionadas",
    "vigencia_inicio", "vigencia_fim", "ativo",
    "content", "metadata",
    "fonte_arquivo", "metodo_extracao", "updated_at",
}

_SCHEMA_ALIQ_ZERO = {
    "ncm", "descricao", "segmento", "cst_pis", "cst_cofins",
    "descricao_cst", "regime_tributario", "natureza_receita",
    "grupo_legal",
    "aliq_pis_simples", "aliq_cofins_simples",
    "aliq_pis_presumido", "aliq_cofins_presumido",
    "aliq_pis_real", "aliq_cofins_real",
    "base_legal", "lei", "fundamentacao_resumida",
    "norma_legal_id", "normas_relacionadas",
    "vigencia_inicio", "vigencia_fim", "ativo",
    "content", "metadata",
    "fonte_arquivo", "metodo_extracao", "updated_at",
}

_SEGMENTOS_VALIDOS = frozenset({
    "Combustíveis", "Medicamentos", "Perfumaria e Higiene", "Veículos",
    "Bebidas Alcoólicas", "Produtos Alimentícios", "Saúde",
    "Aeronaves e Embarcações", "Materiais de Construção",
    "Materiais de Limpeza", "Pneumáticos", "Tintas e Vernizes",
    "Cigarros e Tabaco", "Rações Pet", "Eletrônicos", "Não identificado",
})

_GRUPO_LEGAL_POR_ARQUIVO = {
    "cesta_basica":    "cesta_basica",
    "cesta básica":    "cesta_basica",
    "leite":           "leite_derivados",
    "aeronave":        "aeronaves",
    "embarcacao":      "embarcacoes",
    "embarcação":      "embarcacoes",
    "saude":           "saude",
    "saúde":           "saude",
    "ortopedico":      "ortopedicos",
    "ortopédico":      "ortopedicos",
    "deficiente":      "deficientes",
    "suplemento":      "suplementos",
}


def _inferir_grupo_legal(fonte_arquivo: str) -> str | None:
    if not fonte_arquivo:
        return None
    fl = fonte_arquivo.lower()
    for fragmento, grupo in _GRUPO_LEGAL_POR_ARQUIVO.items():
        if fragmento in fl:
            return grupo
    return None


def _preparar_payload_motor(registro: dict, tabela: str) -> dict | None:
    ncm = registro.get("ncm")
    if not ncm:
        return None

    cst = str(registro.get("cst_pis") or "").strip()

    if tabela == "produtos_tributacao_concentrada" and cst != "04":
        return None
    if tabela == "produtos_aliquota_zero" and cst != "06":
        return None

    extra = registro.get("_motor_extra") or {}
    aliq_pis_pres  = extra.get("aliq_pis_presumido")
    aliq_cof_pres  = extra.get("aliq_cofins_presumido")
    aliq_pis_real  = extra.get("aliq_pis_real")
    aliq_cof_real  = extra.get("aliq_cofins_real")
    regime_trib    = extra.get("regime_tributario", "monofasico")
    fund_resumida  = extra.get("fundamentacao_resumida") or _FUNDAMENTACAO_LOOKUP.get(ncm) or _FUNDAMENTACAO_LOOKUP.get(ncm[:4])

    segmento = registro.get("segmento") or "Não identificado"
    if segmento not in _SEGMENTOS_VALIDOS:
        segmento = "Não identificado"

    payload = {
        "ncm":                   ncm,
        "descricao":             registro.get("descricao") or "Sem descrição",
        "segmento":              segmento,
        "cst_pis":               cst,
        "cst_cofins":            str(registro.get("cst_cofins") or cst).strip(),
        "descricao_cst":         registro.get("descricao_cst"),
        "natureza_receita":      registro.get("natureza_receita"),
        "regime_tributario":     regime_trib,
        "aliq_pis_simples":      extra.get("aliq_pis_simples", 0),
        "aliq_cofins_simples":   extra.get("aliq_cofins_simples", 0),
        "aliq_pis_presumido":    aliq_pis_pres,
        "aliq_cofins_presumido": aliq_cof_pres,
        "aliq_pis_real":         aliq_pis_real,
        "aliq_cofins_real":      aliq_cof_real,
        "base_legal":            registro.get("base_legal"),
        "lei":                   registro.get("lei"),
        "fundamentacao_resumida": fund_resumida,
        "norma_legal_id":        registro.get("norma_legal_id"),
        "normas_relacionadas":   registro.get("normas_relacionadas"),
        "vigencia_inicio":       registro.get("vigencia_inicio") or "1998-01-01",
        "vigencia_fim":          registro.get("vigencia_fim"),
        "ativo":                 registro.get("ativo", True),
        "content":               registro.get("content"),
        "metadata":              registro.get("metadata"),
        "fonte_arquivo":         registro.get("fonte_arquivo"),
        "metodo_extracao":       registro.get("metodo_extracao") or "json_estruturado",
        "updated_at":            registro.get("updated_at") or now(),
    }

    if tabela == "produtos_tributacao_concentrada":
        aliquota_por_pauta = (
            payload["aliq_pis_presumido"] is None and
            payload["aliq_cofins_presumido"] is None
        )
        payload["aliquota_por_pauta"] = aliquota_por_pauta
        if aliquota_por_pauta:
            payload["aliq_pis_presumido"]    = None
            payload["aliq_cofins_presumido"] = None
        return {k: v for k, v in payload.items() if k in _SCHEMA_TRIB_CONC}

    if tabela == "produtos_aliquota_zero":
        payload["aliq_pis_presumido"]    = 0
        payload["aliq_cofins_presumido"] = 0
        payload["aliq_pis_real"]         = 0
        payload["aliq_cofins_real"]      = 0
        payload["grupo_legal"] = (
            registro.get("grupo_legal") or
            _inferir_grupo_legal(registro.get("fonte_arquivo") or "")
        )
        return {k: v for k, v in payload.items() if k in _SCHEMA_ALIQ_ZERO}

    return None


def _upsert_motor_tributario(registros: list, origem: str = "") -> dict:
    if not supabase or not registros:
        return {"trib_conc": 0, "aliq_zero": 0, "erros": []}

    lote_cst04: list = []
    lote_cst06: list = []

    for reg in registros:
        cst = str(reg.get("cst_pis") or "").strip()
        if cst == "04":
            p = _preparar_payload_motor(reg, "produtos_tributacao_concentrada")
            if p:
                lote_cst04.append(p)
        elif cst == "06":
            p = _preparar_payload_motor(reg, "produtos_aliquota_zero")
            if p:
                lote_cst06.append(p)

    resultado = {"trib_conc": 0, "aliq_zero": 0, "erros": []}
    BATCH = 500

    for i in range(0, len(lote_cst04), BATCH):
        lote = lote_cst04[i:i + BATCH]
        try:
            resp = (
                supabase.table("produtos_tributacao_concentrada")
                .upsert(lote, on_conflict="ncm")
                .execute()
            )
            resultado["trib_conc"] += len(resp.data) if resp.data else len(lote)
            log.info(
                f"[motor] {origem} lote {i//BATCH + 1}: "
                f"{len(lote)} registro(s) → produtos_tributacao_concentrada"
            )
        except Exception as e:
            msg = f"motor:cst04:{str(e)[:200]}"
            resultado["erros"].append(msg)
            log.error(f"[motor] {origem} upsert produtos_tributacao_concentrada: {e}")

    for i in range(0, len(lote_cst06), BATCH):
        lote = lote_cst06[i:i + BATCH]
        try:
            resp = (
                supabase.table("produtos_aliquota_zero")
                .upsert(lote, on_conflict="ncm")
                .execute()
            )
            resultado["aliq_zero"] += len(resp.data) if resp.data else len(lote)
            log.info(
                f"[motor] {origem} lote {i//BATCH + 1}: "
                f"{len(lote)} registro(s) → produtos_aliquota_zero"
            )
        except Exception as e:
            msg = f"motor:cst06:{str(e)[:200]}"
            resultado["erros"].append(msg)
            log.error(f"[motor] {origem} upsert produtos_aliquota_zero: {e}")

    log.info(
        f"[motor] {origem} ✓ "
        f"trib_conc={resultado['trib_conc']} | "
        f"aliq_zero={resultado['aliq_zero']} | "
        f"erros={len(resultado['erros'])}"
    )
    return resultado


def _extrair_registros_mono(data: dict, filename: str) -> list:
    grupos = data.get("produtos_monofasicos")
    if not grupos or not isinstance(grupos, list):
        log.warning(f"[mono] '{filename}': chave 'produtos_monofasicos' ausente ou vazia")
        return []

    registros: list = []
    total_invalidos = 0

    for grupo in grupos:
        if not isinstance(grupo, dict):
            continue
        group_label = str(grupo.get("group") or "")
        items       = grupo.get("items") or []

        if not isinstance(items, list):
            log.warning(f"[mono] '{filename}' grupo '{group_label}': 'items' não é lista")
            continue

        log.info(f"[mono] '{filename}' grupo '{group_label}': {len(items)} item(ns)")

        for item in items:
            if not isinstance(item, dict):
                continue
            registro = _mapear_item_monofasico(item, group_label, filename)
            if registro:
                registros.append(registro)
            else:
                total_invalidos += 1

    log.info(
        f"[mono] '{filename}': {len(registros)} registro(s) válidos | "
        f"{total_invalidos} ignorados (NCM inválido)"
    )
    return registros


async def _run_upsert_monofasicos(folder_id: str):
    svc = _svc()
    if not svc:
        log.error("[mono] Falha na autenticação Google Drive")
        return

    try:
        files = (
            svc.files()
            .list(
                q=(
                    f"'{folder_id}' in parents "
                    f"and mimeType='application/json' "
                    f"and trashed=false"
                ),
                fields="files(id, name)",
                pageSize=100,
            )
            .execute()
            .get("files", [])
        )
    except Exception as e:
        log.error(f"[mono] Listar pasta Drive: {e}")
        return

    log.info(f"[mono] {len(files)} arquivo(s) JSON na pasta {folder_id}")

    total_inseridos = 0
    total_erros     = 0

    for file_meta in files:
        file_id  = file_meta["id"]
        filename = file_meta["name"]
        log.info(f"[mono] baixando '{filename}'…")

        try:
            buf = io.BytesIO()
            dl  = MediaIoBaseDownload(buf, svc.files().get_media(fileId=file_id))
            done = False
            while not done:
                _, done = dl.next_chunk()
            data_json = json.loads(buf.getvalue().decode("utf-8"))
        except Exception as e:
            log.error(f"[mono] Falha ao baixar '{filename}': {e}")
            total_erros += 1
            continue

        registros = _extrair_registros_mono(data_json, filename)
        if not registros:
            log.warning(f"[mono] '{filename}': nenhum registro extraído — ignorado")
            continue

        registros = dedup(registros)
        log.info(f"[mono] '{filename}': {len(registros)} registro(s) após dedup")

        # ── v3.9: NÃO filtra aqui — preserva _motor_extra para o motor ───────
        # O filtro é aplicado por lote apenas no momento do upsert em
        # produtos_monofasicos, garantindo que _upsert_motor_tributario
        # receba o lote original com _motor_extra intacto.

        BATCH = 500
        for i in range(0, len(registros), BATCH):
            lote = registros[i:i + BATCH]

            # Filtra APENAS para o upsert em produtos_monofasicos
            lote_mono = [_filtrar_payload_mono(r) for r in lote]

            try:
                resp = (
                    supabase.table("produtos_monofasicos")
                    .upsert(lote_mono, on_conflict="ncm")
                    .execute()
                )
                n_ok = len(resp.data) if resp.data else len(lote_mono)
                total_inseridos += n_ok
                log.info(
                    f"[mono] '{filename}' lote {i//BATCH + 1}: "
                    f"{n_ok} registro(s) upserted em produtos_monofasicos"
                )
                # Motor recebe lote original (com _motor_extra intacto)
                _upsert_motor_tributario(lote, origem=f"'{filename}' lote {i//BATCH + 1}")
            except Exception as e:
                log.error(
                    f"[mono] '{filename}' lote {i//BATCH + 1}: "
                    f"upsert erro — {e}"
                )
                total_erros += len(lote)

    log.info(
        f"[mono] ✓ CONCLUÍDO — "
        f"total inseridos/atualizados: {total_inseridos} | "
        f"erros: {total_erros}"
    )


@app.post("/upsert-icms-st")
async def upsert_icms_st(
    background_tasks: BackgroundTasks,
    folder_id: Optional[str] = None,
):
    if not supabase:
        raise HTTPException(503, "Supabase não configurado")
    if not HAS_GDRIVE:
        raise HTTPException(503, "Biblioteca Google Drive não disponível")

    fid = folder_id or DRIVE_FOLDER_ID_JSONS
    if not fid:
        raise HTTPException(
            400,
            "Informe folder_id na query ou defina DRIVE_FOLDER_ID_JSONS no ambiente"
        )

    background_tasks.add_task(_run_upsert_icms_jsons, fid)
    return {
        "status":    "iniciado",
        "folder_id": fid,
        "tabelas":   _TABLE_ORDER,
        "info":      "Acompanhe os logs do Railway para progresso detalhado",
    }


@app.get("/status-icms-st")
def status_icms_st():
    if not supabase:
        return {"erro": "Supabase não configurado"}
    resultado = {}
    for table in _TABLE_ORDER:
        try:
            resp = supabase.table(table).select("id", count="exact").execute()
            resultado[table] = resp.count
        except Exception as e:
            resultado[table] = f"erro: {e}"
    resultado["timestamp"] = datetime.utcnow().isoformat()
    return resultado


@app.post("/upsert-monofasicos")
async def upsert_monofasicos(
    background_tasks: BackgroundTasks,
    folder_id: Optional[str] = None,
):
    if not supabase:
        raise HTTPException(503, "Supabase não configurado")
    if not HAS_GDRIVE:
        raise HTTPException(503, "Biblioteca Google Drive não disponível")

    fid = folder_id or DRIVE_FOLDER_ID_MONO or DRIVE_FOLDER_ID_JSONS
    if not fid:
        raise HTTPException(
            400,
            "Informe folder_id na query ou defina DRIVE_FOLDER_ID_MONO no ambiente"
        )

    background_tasks.add_task(_run_upsert_monofasicos, fid)
    return {
        "status":    "iniciado",
        "folder_id": fid,
        "tabela":    "produtos_monofasicos",
        "info":      "Acompanhe os logs do Railway para progresso detalhado",
    }


@app.get("/status-monofasicos")
def status_monofasicos():
    if not supabase:
        return {"erro": "Supabase não configurado"}
    try:
        total_resp = (
            supabase.table("produtos_monofasicos")
            .select("id", count="exact")
            .execute()
        )
        total = total_resp.count

        ativos_resp = (
            supabase.table("produtos_monofasicos")
            .select("id", count="exact")
            .eq("ativo", True)
            .execute()
        )
        ativos = ativos_resp.count

        recentes_resp = (
            supabase.table("produtos_monofasicos")
            .select("ncm, descricao, segmento, cst_pis, fonte_arquivo, updated_at")
            .order("updated_at", desc=True)
            .limit(5)
            .execute()
        )

        return {
            "total":     total,
            "ativos":    ativos,
            "inativos":  (total or 0) - (ativos or 0),
            "recentes":  recentes_resp.data or [],
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        return {"erro": str(e)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        reload=True,
    )
