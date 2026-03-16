"""
Microserviço de Extração Fiscal v2.1
=====================================
Três camadas: Determinístico → Regex → Semântico (LLM)
Mapeado para a estrutura REAL das tabelas no Supabase.
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

SUPABASE_URL            = os.getenv("SUPABASE_URL","")
SUPABASE_KEY            = os.getenv("SUPABASE_KEY","")
OPENAI_API_KEY          = os.getenv("OPENAI_API_KEY","")
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH","credentials.json")
DRIVE_FOLDER_ID         = os.getenv("DRIVE_FOLDER_ID","")

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

app = FastAPI(title="Extrator Fiscal v2.1", version="2.1.0")
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_methods=["*"],allow_headers=["*"])

supabase = create_client(SUPABASE_URL, SUPABASE_KEY) if HAS_SB and SUPABASE_URL and SUPABASE_KEY else None

# ── Schemas de entrada ───────────────────────────────────────────────────────
class ExtrairRequest(BaseModel):
    pdf_base64:    Optional[str] = None
    file_name:     str = "arquivo"
    file_type:     str = "pdf"
    folder_name:   str = ""
    forcar_regime: Optional[str] = None

# ── Constantes ───────────────────────────────────────────────────────────────
RE_NCM       = re.compile(r'\b(\d{4}[\.\-]?\d{0,2}[\.\-]?\d{0,2}[\.\-]?\d{0,2})\b')
RE_CEST      = re.compile(r'\b(\d{2}\.\d{3}\.\d{2})\b')
RE_CFOP      = re.compile(r'\b([56]\d{3})\b')
RE_VIGENCIA  = re.compile(r'(?:vigência|efeitos?|a partir de)\s*[:\s]*(\d{2}[\/\.\-]\d{2}[\/\.\-]\d{4})',re.I)
RE_REVOGACAO = re.compile(r'(?:revogad[ao]|até)\s*[:\s]*(\d{2}[\/\.\-]\d{2}[\/\.\-]\d{4})',re.I)
RE_CONV      = re.compile(r'[Cc]onvênio\s+ICMS\s+(?:n[oº]?\s*)?(\d+[/\-]\d+)')
RE_PROT      = re.compile(r'[Pp]rotocolo\s+ICMS\s+(?:n[oº]?\s*)?(\d+[/\-]\d+)')
RE_LEI       = re.compile(r'[Ll]ei\s+(?:[Cc]omplementar\s+)?(?:n[oº]?\s*)?(\d+[\.\d]*[/\-]\d+)')
RE_DECRETO   = re.compile(r'[Dd]ecreto\s+(?:n[oº]?\s*)?(\d+[\.\d]*[/\-]\d+)')
RE_NAT       = re.compile(r'\b([12]\d{2})\b')

CST_DESC = {
    "01":"Tributável alíquota básica",
    "02":"Tributável alíquota diferenciada",
    "04":"Monofásico — revenda alíquota zero",
    "06":"Alíquota zero",
    "07":"Isento",
    "060":"ICMS-ST cobrado anteriormente",
    "500":"CSOSN — ST cobrado anteriormente",
}

PADROES_MONO = [r"monof[aá]sico",r"pis[/\s]+cofins",r"lei\s+10\.147",r"lei\s+10\.485",
                r"lei\s+10\.560",r"natureza\s+de\s+receita",r"alíquota\s+zero",r"cst[_\s]*0[24]"]
PADROES_ST   = [r"substitui[çc][aã]o\s+tribut",r"icms[_\s\-]+st",
                r"cest\s*[:\s]*\d{2}\.\d{3}\.\d{2}",r"protocolo\s+icms",
                r"conv[eê]nio\s+icms",r"mva|margem\s+de\s+valor",r"cfop\s+5405|cfop\s+6404"]
SEGMENTOS = {
    "Autopeças":             ["autopeça","autopeca","amortecedor","freio"],
    "Combustíveis":          ["combustível","gasolina","diesel","glp","lubrificante"],
    "Medicamentos":          ["medicamento","farmacê","princípio ativo","genérico"],
    "Produtos Alimentícios": ["aliment","bebida","laticín","chocolate","farinha","carne"],
    "Perfumaria e Higiene":  ["perfum","cosmétic","xampu","higiene","sabão","desodorante"],
    "Eletrônicos":           ["eletrônico","smartphone","televisor","computador"],
    "Materiais de Construção":["construção","cimento","tijolo","cerâmica","vergalhão"],
    "Materiais de Limpeza":  ["limpeza","detergente","sanitizante","alvejante"],
    "Pneumáticos":           ["pneumático","pneu","câmara de ar"],
    "Tintas e Vernizes":     ["tinta","verniz","resina","esmalte"],
    "Bebidas Alcoólicas":    ["cerveja","vinho","aguardente","cachaça"],
    "Cigarros e Tabaco":     ["cigarro","tabaco","fumo","charuto"],
    "Veículos":              ["veículo automotor","automóvel","caminhão","motocicleta"],
    "Rações Pet":            ["ração","pet","animal doméstico"],
}

# ── Helpers ──────────────────────────────────────────────────────────────────
def limpar_ncm(r):
    d=re.sub(r'[^\d]','',r); return d if 4<=len(d)<=8 else None

def nd(raw):
    if not raw: return None
    for f in ('%d/%m/%Y','%d.%m.%Y','%d-%m-%Y','%Y-%m-%d','%Y/%m/%d'):
        try: return datetime.strptime(raw.strip(),f).strftime('%Y-%m-%d')
        except: pass
    return None

def detectar_regime(texto,folder=""):
    fl=folder.lower()
    if re.search(r'\bmono[_\s]|monofas',fl): return "monofasico",1.0
    if re.search(r'\bicms[_\s]*st[_\s]|^st[_\s]',fl): return "icms_st",1.0
    tl=texto.lower()
    sm=sum(1 for p in PADROES_MONO if re.search(p,tl))
    ss=sum(1 for p in PADROES_ST   if re.search(p,tl))
    t=sm+ss or 1
    return ("monofasico",round(sm/t,2)) if sm>ss else ("icms_st",round(ss/t,2))

def detectar_segmento(texto):
    tl=texto[:5000].lower()
    for seg,pals in SEGMENTOS.items():
        if any(p in tl for p in pals): return seg
    return "Não identificado"

def fund(texto):
    convs=RE_CONV.findall(texto); prots=RE_PROT.findall(texto)
    leis=RE_LEI.findall(texto);   decs=RE_DECRETO.findall(texto)
    convenio=None; ricms=None
    if convs: convenio="Convênio ICMS "+", ".join(convs[:3])
    if prots: convenio=(convenio+" | " if convenio else "")+"Protocolo ICMS "+", ".join(prots[:2])
    if leis:  ricms="Lei "+", ".join(leis[:2])
    if decs:  ricms=(ricms+" | " if ricms else "")+"Decreto "+", ".join(decs[:2])
    return {"convenio":convenio,"ricms":ricms}

def now(): return datetime.utcnow().isoformat()

# ──────────────────────────────────────────────────────────────────────────────
# BUILDERS — nomes de coluna REAIS das tabelas
# ──────────────────────────────────────────────────────────────────────────────

def mk_mono(ncm,descricao,segmento,cst_pis,natureza,base_legal,lei,
            vigencia_inicio,vigencia_fim,fonte,metodo):
    """
    Colunas reais de produtos_monofasicos usadas aqui:
    ncm, descricao, base_legal, lei, vigencia_inicio, vigencia_fim, ativo,
    cst_pis, cst_cofins, natureza_receita,
    aliq_pis_presumido, aliq_cofins_presumido,
    aliq_pis_real, aliq_cofins_real,
    segmento, descricao_cst, fonte_arquivo, metodo_extracao, updated_at
    """
    cst = cst_pis or "04"
    return {
        "ncm":                   ncm,
        "descricao":             descricao,
        "base_legal":            base_legal,
        "lei":                   lei,
        "vigencia_inicio":       vigencia_inicio,
        "vigencia_fim":          vigencia_fim,
        "ativo":                 vigencia_fim is None,
        "cst_pis":               cst,
        "cst_cofins":            cst,
        "natureza_receita":      natureza,
        "aliq_pis_presumido":    0.65,
        "aliq_cofins_presumido": 3.0,
        "aliq_pis_real":         1.65,
        "aliq_cofins_real":      7.6,
        "aliq_pis_simples":      None,
        "aliq_cofins_simples":   None,
        "segmento":              segmento,
        "descricao_cst":         CST_DESC.get(cst),
        "fonte_arquivo":         fonte,
        "metodo_extracao":       metodo,
        "updated_at":            now(),
    }

def mk_st(ncm,cest,descricao,segmento,cst,csosn,
          cfop_interno,cfop_interestadual,
          fund_conv,fund_ricms,
          vigencia_inicio,vigencia_fim,fonte,metodo):
    """
    Colunas reais de produtos_icms_st usadas aqui:
    ncm, cest, descricao, segmento,
    cst_icms, csosn, cst,
    cfop_interno, cfop_externo, cfop_interestadual,
    fundamentacao_convenio, fundamentacao_ricms,  ← SEM _ma
    vigencia_inicio, vigencia_fim, ativo,
    descricao_cst, fonte_arquivo, metodo_extracao, updated_at
    """
    return {
        "ncm":                   ncm,
        "cest":                  cest,
        "descricao":             descricao,
        "segmento":              segmento,
        "cst_icms":              cst or "060",
        "csosn":                 csosn or "500",
        "cst":                   cst or "060",
        "cfop_interno":          cfop_interno  or "5405",
        "cfop_externo":          cfop_interestadual or "6404",
        "cfop_interestadual":    cfop_interestadual or "6404",
        "fundamentacao_convenio":fund_conv,
        "fundamentacao_ricms":   fund_ricms,
        "vigencia_inicio":       vigencia_inicio,
        "vigencia_fim":          vigencia_fim,
        "ativo":                 vigencia_fim is None,
        "descricao_cst":         CST_DESC.get("060"),
        "fonte_arquivo":         fonte,
        "metodo_extracao":       metodo,
        "updated_at":            now(),
    }

# ── Camada 1 — Tabelas estruturadas ──────────────────────────────────────────
def extrair_tabela_pdf(page,regime,segmento,fonte):
    items=[]
    for table in (page.extract_tables() or []):
        if not table or len(table)<2: continue
        hdr=[str(c).lower().strip() if c else "" for c in (table[0] or [])]
        col={
            "ncm":   next((i for i,h in enumerate(hdr) if "ncm" in h or "sh" in h),None),
            "cest":  next((i for i,h in enumerate(hdr) if "cest" in h),None),
            "desc":  next((i for i,h in enumerate(hdr) if any(w in h for w in ["descri","produto","mercad"])),None),
            "cst":   next((i for i,h in enumerate(hdr) if h in ["cst","cst_icms","cst_pis"]),None),
            "cfop_i":next((i for i,h in enumerate(hdr) if "cfop_i" in h or "interno" in h),None),
            "cfop_e":next((i for i,h in enumerate(hdr) if "cfop_e" in h or "interes" in h),None),
            "vig":   next((i for i,h in enumerate(hdr) if any(w in h for w in ["vigên","inicio","data_ini"])),None),
            "rev":   next((i for i,h in enumerate(hdr) if any(w in h for w in ["revog","vigência_fim","data_fim"])),None),
            "nat":   next((i for i,h in enumerate(hdr) if "natureza" in h),None),
            "lei":   next((i for i,h in enumerate(hdr) if "lei" in h),None),
        }
        if col["ncm"] is None:
            cts=[sum(1 for r in table[1:] if r and ci<len(r) and r[ci] and RE_NCM.search(str(r[ci]))) for ci in range(len(hdr))]
            if cts and max(cts)>0: col["ncm"]=cts.index(max(cts))
        if col["ncm"] is None: continue

        for row in table[1:]:
            if not row or all(not c for c in row): continue
            def cel(k):
                idx=col.get(k)
                return str(row[idx]).strip() if idx is not None and idx<len(row) and row[idx] else ""
            linha=" ".join(str(c) for c in row if c)
            for nr in (RE_NCM.findall(cel("ncm")) or RE_NCM.findall(linha)):
                ncm=limpar_ncm(nr)
                if not ncm: continue
                cest_m=RE_CEST.search(cel("cest")) or RE_CEST.search(linha)
                vig_m=RE_VIGENCIA.search(cel("vig") or linha)
                rev_m=RE_REVOGACAO.search(cel("rev") or linha)
                f=fund(linha)
                nat_m=RE_NAT.search(cel("nat"))
                if regime=="monofasico":
                    items.append(mk_mono(ncm,cel("desc") or None,segmento,
                        cel("cst") or None,nat_m.group(1) if nat_m else None,
                        f["ricms"] or f["convenio"],cel("lei") or None,
                        nd(vig_m.group(1)) if vig_m else None,
                        nd(rev_m.group(1)) if rev_m else None,fonte,"tabela"))
                else:
                    cfop_m=RE_CFOP.search(linha)
                    items.append(mk_st(ncm,cest_m.group(1) if cest_m else None,
                        cel("desc") or None,segmento,cel("cst") or None,None,
                        cel("cfop_i") or (cfop_m.group(1) if cfop_m else "5405"),
                        cel("cfop_e") or "6404",
                        f["convenio"],f["ricms"],
                        nd(vig_m.group(1)) if vig_m else None,
                        nd(rev_m.group(1)) if rev_m else None,fonte,"tabela"))
    return items

def extrair_xlsx(conteudo,regime,fonte):
    if not HAS_XLSX or not HAS_PANDAS: return []
    items=[]
    try:
        for _,df in pd.read_excel(io.BytesIO(conteudo),sheet_name=None).items():
            df.columns=[str(c).lower().strip() for c in df.columns]
            col_ncm=next((c for c in df.columns if "ncm" in c),None)
            if not col_ncm: continue
            for _,row in df.iterrows():
                ncm=limpar_ncm(str(row.get(col_ncm,"")))
                if not ncm: continue
                def g(keys):
                    for k in ([keys] if isinstance(keys,str) else keys):
                        v=next((row[c] for c in df.columns if k in c),None)
                        if v is not None and pd.notna(v): return str(v).strip()
                    return None
                if regime=="monofasico":
                    items.append(mk_mono(ncm,g("descri"),g("segmento"),g("cst"),g("natureza"),
                        g("base_legal") or g("lei"),g("lei"),nd(g("vigencia_inicio")),nd(g("vigencia_fim")),fonte,"tabela_xlsx"))
                else:
                    items.append(mk_st(ncm,g("cest"),g("descri"),g("segmento"),g("cst"),g("csosn"),
                        g(["cfop_interno","cfop_i"]),g(["cfop_interestadual","cfop_e"]),
                        g("fundamentacao_convenio"),g("fundamentacao_ricms"),
                        nd(g("vigencia_inicio")),nd(g("vigencia_fim")),fonte,"tabela_xlsx"))
    except Exception as e: log.error(f"XLSX: {e}")
    return items

def extrair_csv(conteudo,regime,fonte):
    if not HAS_PANDAS: return []
    try:
        for enc in ("utf-8","latin-1","cp1252"):
            try: df=pd.read_csv(io.BytesIO(conteudo),encoding=enc,sep=None,engine="python"); break
            except: pass
        else: return []
        df.columns=[str(c).lower().strip() for c in df.columns]
        col_ncm=next((c for c in df.columns if "ncm" in c),None)
        if not col_ncm: return []
        items=[]
        for _,row in df.iterrows():
            ncm=limpar_ncm(str(row.get(col_ncm,"")))
            if not ncm: continue
            if regime=="monofasico":
                items.append(mk_mono(ncm,None,None,"04",None,None,None,None,None,fonte,"csv"))
            else:
                items.append(mk_st(ncm,None,None,None,"060",None,"5405","6404",None,None,None,None,fonte,"csv"))
        return items
    except Exception as e: log.error(f"CSV: {e}"); return []

# ── Camada 2 — Regex texto corrido ───────────────────────────────────────────
def extrair_regex(texto,regime,segmento,fonte):
    items=[]
    linhas=texto.split("\n")
    f=fund(texto)
    for i,linha in enumerate(linhas):
        for nr in RE_NCM.findall(linha):
            ncm=limpar_ncm(nr)
            if not ncm: continue
            ctx=" ".join(linhas[max(0,i-2):i+3])
            cest_m=RE_CEST.search(ctx)
            partes=linha.split(nr,1)
            desc=partes[1].strip()[:300] if len(partes)>1 else None
            vig_m=RE_VIGENCIA.search(ctx)
            rev_m=RE_REVOGACAO.search(ctx)
            nat_m=RE_NAT.search(ctx)
            if regime=="monofasico":
                items.append(mk_mono(ncm,desc,segmento,"04",
                    nat_m.group(1) if nat_m else None,
                    f["ricms"] or f["convenio"],None,
                    nd(vig_m.group(1)) if vig_m else None,
                    nd(rev_m.group(1)) if rev_m else None,fonte,"regex"))
            else:
                cfop_m=RE_CFOP.search(ctx)
                items.append(mk_st(ncm,cest_m.group(1) if cest_m else None,desc,segmento,
                    "060",None,cfop_m.group(1) if cfop_m else "5405","6404",
                    f["convenio"],f["ricms"],
                    nd(vig_m.group(1)) if vig_m else None,
                    nd(rev_m.group(1)) if rev_m else None,fonte,"regex"))
    return items

# ── Camada 3 — LLM ───────────────────────────────────────────────────────────
async def extrair_llm(texto,regime,fonte):
    if not OPENAI_API_KEY or not HAS_HTTPX: return []
    s_mono='{"ncm":"XXXXXXXX","descricao":"...","cst_pis":"04","natureza_receita":"XXX ou null","base_legal":"Lei ... ou null","segmento":"..."}'
    s_st  ='{"ncm":"XXXXXXXX","cest":"XX.XXX.XX ou null","descricao":"...","segmento":"...","fundamentacao_convenio":"Conv. ICMS ... ou null"}'
    schema=s_mono if regime=="monofasico" else s_st
    label ="PIS/Cofins monofásico" if regime=="monofasico" else "ICMS Substituição Tributária"
    try:
        async with httpx.AsyncClient(timeout=90) as c:
            r=await c.post("https://api.openai.com/v1/chat/completions",
                headers={"Authorization":f"Bearer {OPENAI_API_KEY}"},
                json={"model":"gpt-4o-mini","max_tokens":4000,"messages":[
                    {"role":"system","content":"Extrator fiscal. Retorne apenas JSON array válido."},
                    {"role":"user","content":f"Extraia NCMs de {label}. Array JSON:\n[{schema}]\nTexto:\n{texto[:10000]}"}
                ]})
        raw=re.sub(r"```json|```","",r.json()["choices"][0]["message"]["content"].strip()).strip()
        parsed=json.loads(raw)
        if not isinstance(parsed,list): return []
        items=[]
        for p in parsed:
            ncm=limpar_ncm(str(p.get("ncm","")))
            if not ncm: continue
            if regime=="monofasico":
                items.append(mk_mono(ncm,p.get("descricao"),p.get("segmento"),
                    p.get("cst_pis","04"),p.get("natureza_receita"),p.get("base_legal"),None,None,None,fonte,"llm"))
            else:
                items.append(mk_st(ncm,p.get("cest"),p.get("descricao"),p.get("segmento"),
                    "060",None,"5405","6404",p.get("fundamentacao_convenio"),None,None,None,fonte,"llm"))
        return items
    except Exception as e: log.error(f"LLM: {e}"); return []

# ── Dedup ─────────────────────────────────────────────────────────────────────
def dedup(items):
    seen={}
    for item in items:
        k=item["ncm"]
        if k not in seen or sum(1 for v in item.values() if v)>sum(1 for v in seen[k].values() if v):
            seen[k]=item
    return list(seen.values())

# ── Persistência ──────────────────────────────────────────────────────────────
def salvar(mono,st):
    if not supabase: return {"mono":0,"st":0,"erros":["Supabase não configurado"]}
    r={"mono":0,"st":0,"erros":[]}
    if mono:
        try: supabase.table("produtos_monofasicos").upsert(mono,on_conflict="ncm").execute(); r["mono"]=len(mono)
        except Exception as e: r["erros"].append(f"mono:{str(e)[:200]}"); log.error(e)
    if st:
        try: supabase.table("produtos_icms_st").upsert(st,on_conflict="ncm").execute(); r["st"]=len(st)
        except Exception as e: r["erros"].append(f"st:{str(e)[:200]}"); log.error(e)
    return r

def log_proc(arquivo,folder,tipo,metodo,n_mono,n_st,avisos,erros,ms):
    if not supabase: return
    try:
        supabase.table("log_processamento").insert({
            "arquivo":arquivo,"folder_name":folder,"tipo_arquivo":tipo,
            "metodo_extracao":metodo,"total_monofasicos":n_mono,"total_icms_st":n_st,
            "avisos":avisos,"erros":erros,"duracao_ms":ms
        }).execute()
    except: pass

# ── Pipeline ──────────────────────────────────────────────────────────────────
async def processar(conteudo,file_name,file_type,folder_name,forcar_regime):
    t0=datetime.utcnow()
    mono,st,avisos=[],[],[]
    txt_total=""
    metodo="tabela"
    ext=file_type.lower().strip(".")

    if ext=="xlsx":
        regime,_=detectar_regime("",folder_name)
        if forcar_regime: regime=forcar_regime
        for item in extrair_xlsx(conteudo,regime,file_name):
            (mono if regime=="monofasico" else st).append(item)
        metodo="tabela_xlsx"

    elif ext=="csv":
        regime,_=detectar_regime("",folder_name)
        if forcar_regime: regime=forcar_regime
        for item in extrair_csv(conteudo,regime,file_name):
            (mono if regime=="monofasico" else st).append(item)
        metodo="csv"

    elif ext=="docx" and HAS_DOCX:
        doc=Docx(io.BytesIO(conteudo))
        txt_total="\n".join(p.text for p in doc.paragraphs)
        regime,_=detectar_regime(txt_total,folder_name)
        if forcar_regime: regime=forcar_regime
        seg=detectar_segmento(txt_total)
        for item in extrair_regex(txt_total,regime,seg,file_name):
            (mono if regime=="monofasico" else st).append(item)
        metodo="regex"

    elif ext=="pdf" and HAS_PDF:
        pags_tab=0; txts=[]
        with pdfplumber.open(io.BytesIO(conteudo)) as pdf:
            log.info(f"PDF '{file_name}' — {len(pdf.pages)} pgs")
            for n,page in enumerate(pdf.pages,1):
                txt=page.extract_text() or ""
                txts.append(txt)
                regime,_=detectar_regime(txt,folder_name if n==1 else "")
                if forcar_regime: regime=forcar_regime
                seg=detectar_segmento(txt)
                tab=extrair_tabela_pdf(page,regime,seg,file_name)
                if tab:
                    pags_tab+=1
                    for item in tab: (mono if regime=="monofasico" else st).append(item)
                else:
                    for item in extrair_regex(txt,regime,seg,file_name):
                        (mono if regime=="monofasico" else st).append(item)
        txt_total="\n".join(txts)
        metodo="tabela" if pags_tab>0 else "regex"
    else:
        avisos.append(f"Formato '{ext}' não suportado ou biblioteca ausente")

    if not mono and not st and txt_total:
        avisos.append("Camadas 1+2 sem NCMs — acionando LLM")
        regime,_=detectar_regime(txt_total,folder_name)
        if forcar_regime: regime=forcar_regime
        for item in await extrair_llm(txt_total,regime,file_name):
            (mono if regime=="monofasico" else st).append(item)
        metodo="llm"

    mono=dedup(mono); st=dedup(st)
    if not mono and not st: avisos.append("Nenhum NCM extraído")
    log.info(f"'{file_name}' → {len(mono)} mono | {len(st)} ST | {metodo}")
    ms=int((datetime.utcnow()-t0).total_seconds()*1000)
    return {"mono":mono,"st":st,"metodo":metodo,"avisos":avisos,"ms":ms}

# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status":"ok","versao":"2.1.0","supabase":supabase is not None,
            "pdf":HAS_PDF,"xlsx":HAS_XLSX,"docx":HAS_DOCX,"gdrive":HAS_GDRIVE}

@app.get("/status")
def status_db():
    if not supabase: return {"erro":"Supabase não configurado"}
    try:
        m=supabase.table("produtos_monofasicos").select("ncm",count="exact").execute()
        s=supabase.table("produtos_icms_st").select("ncm",count="exact").execute()
        return {"produtos_monofasicos":m.count,"produtos_icms_st":s.count,
                "timestamp":datetime.utcnow().isoformat()}
    except Exception as e: return {"erro":str(e)}

@app.post("/extrair-ncms")
async def extrair_ncms(req:ExtrairRequest):
    if not req.pdf_base64: raise HTTPException(400,"pdf_base64 obrigatório")
    try: conteudo=base64.b64decode(req.pdf_base64)
    except: raise HTTPException(400,"pdf_base64 inválido")
    res=await processar(conteudo,req.file_name,req.file_type,req.folder_name,req.forcar_regime)
    salvos=salvar(res["mono"],res["st"])
    log_proc(req.file_name,req.folder_name,req.file_type,res["metodo"],
             len(res["mono"]),len(res["st"]),res["avisos"],salvos.get("erros",[]),res["ms"])
    return {"monofasicos":res["mono"],"icms_st":res["st"],
            "total_monofasicos":len(res["mono"]),"total_icms_st":len(res["st"]),
            "metodo_principal":res["metodo"],"arquivo":req.file_name,
            "avisos":res["avisos"],"erros":salvos.get("erros",[]),"duracao_ms":res["ms"]}

# Drive
def _svc():
    if not HAS_GDRIVE: return None
    try:
        import json, tempfile
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
        return build("drive","v3",credentials=creds)
    except Exception as e: log.error(f"Drive: {e}"); return None

@app.post("/processar-drive")
async def processar_drive(background_tasks:BackgroundTasks,folder_id:Optional[str]=None):
    fid=folder_id or DRIVE_FOLDER_ID
    if not fid: raise HTTPException(400,"folder_id não informado")
    background_tasks.add_task(_pasta,fid)
    return {"status":"iniciado","folder_id":fid}

async def _pasta(folder_id):
    svc=_svc()
    if not svc: return
    TIPOS={"application/pdf":"pdf",
           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":"xlsx",
           "text/csv":"csv",
           "application/vnd.openxmlformats-officedocument.wordprocessingml.document":"docx"}
    try:
        arqs=svc.files().list(q=f"'{folder_id}' in parents and trashed=false",
            fields="files(id,name,mimeType)",pageSize=200).execute().get("files",[])
    except Exception as e: log.error(f"Listar Drive: {e}"); return
    for arq in arqs:
        ext=TIPOS.get(arq["mimeType"])
        if not ext: continue
        try:
            buf=io.BytesIO()
            dl=MediaIoBaseDownload(buf,svc.files().get_media(fileId=arq["id"]))
            done=False
            while not done: _,done=dl.next_chunk()
            conteudo=buf.getvalue()
        except Exception as e: log.error(f"Baixar {arq['name']}: {e}"); continue
        res=await processar(conteudo,arq["name"],ext,arq["name"],None)
        salvos=salvar(res["mono"],res["st"])
        log_proc(arq["name"],folder_id,ext,res["metodo"],
                 len(res["mono"]),len(res["st"]),res["avisos"],salvos.get("erros",[]),res["ms"])

if __name__=="__main__":
    import uvicorn
    uvicorn.run("main:app",host="0.0.0.0",port=int(os.getenv("PORT",8000)),reload=True)
