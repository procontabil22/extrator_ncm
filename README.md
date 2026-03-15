# Extrator NCM — Microserviço FastAPI

Microserviço de **extração determinística de NCMs** de PDFs legislativos/fiscais.
Substitui a abordagem RAG + LLM puro por parsing estruturado com `pdfplumber`.

## Como funciona

```
PDF (base64) → pdfplumber extrai tabelas → regex detecta NCM/CEST
                     ↓ (sem tabelas)
              extração de texto corrido → regex NCM
                     ↓ (nenhum resultado)
              fallback GPT-4o-mini (opcional)
```

## Instalação local

```bash
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Teste rápido:
```bash
curl http://localhost:8000/health
```

## Deploy no Railway (gratuito)

1. Crie conta em https://railway.app
2. Novo projeto → Deploy from GitHub (ou upload direto)
3. O Railway detecta o `Procfile` automaticamente
4. Adicione variável de ambiente: `OPENAI_API_KEY=sk-...`
5. Copie a URL gerada (ex: `https://extrator-ncm.up.railway.app`)

## Deploy no Render (gratuito)

1. Crie conta em https://render.com
2. New Web Service → conecte o repositório
3. Build command: `pip install -r requirements.txt`
4. Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`

## Endpoint

### POST /extrair-ncms

**Request:**
```json
{
  "pdf_base64": "JVBERi0x...",
  "file_name": "convenio_icms_142_2018.pdf",
  "folder_name": "icms_st_bebidas",
  "forcar_regime": null
}
```

**Response:**
```json
{
  "items": [
    {
      "regime": "icms_st",
      "ativo": true,
      "ncm": "22030000",
      "descricao": "Cervejas de malte",
      "cest": "03.021.00",
      "segmento": "Bebidas",
      "cfop_interno": "5405",
      "cfop_interestadual": "6404",
      "cst_pis": null,
      "cst_cofins": null,
      "natureza_receita": null,
      "norma_legal": null,
      "fonte_pdf": "convenio_icms_142_2018.pdf",
      "metodo_extracao": "tabela"
    }
  ],
  "total": 1,
  "metodo_principal": "tabela",
  "arquivo": "convenio_icms_142_2018.pdf",
  "avisos": []
}
```

## Integração com n8n

No n8n, substitua os nós de:
- `Extrai Texto PDF Estruturado1`
- `Chunking Estruturado1`
- `Montar Payload OpenAI1`
- `OpenAI — Extrai NCMs1`
- `Parse NCMs1`

Por um único **nó HTTP Request**:
- Method: POST
- URL: `https://sua-url.railway.app/extrair-ncms`
- Body (JSON):
```json
{
  "pdf_base64": "={{ $binary.data.toString('base64') }}",
  "file_name": "={{ $('Salvar Metadados1').first().json.file_name }}",
  "folder_name": "={{ $('Salvar Metadados1').first().json.folder_name }}"
}
```

Depois use um **nó Code** para transformar `response.items` no formato do lote Supabase.
