-- =============================================================================
-- Fix v3.15 — Remove normas fantasma e vincula NCM 30039056
-- Executar no Supabase SQL Editor ANTES de fazer deploy do main.py v3.15
-- =============================================================================

-- 1. Remove as 24 normas fantasma de Lei 10.485 com ano != 2002
DELETE FROM normas_legais_pis_cofins
WHERE tipo_norma = 'Lei'
  AND numero = '10.485'
  AND ano != 2002;

-- 2. Vincula o NCM 30039056 à Lei 10.147/2000 (que já existe na tabela)
UPDATE produtos_monofasicos
SET
    norma_legal_id = (
        SELECT id FROM normas_legais_pis_cofins
        WHERE tipo_norma = 'Lei' AND numero = '10.147' AND ano = 2000
        LIMIT 1
    ),
    normas_relacionadas = ARRAY(
        SELECT id FROM normas_legais_pis_cofins
        WHERE tipo_norma = 'Lei' AND numero = '10.147' AND ano = 2000
        LIMIT 1
    ),
    lei        = 'Lei 10.147/2000',
    updated_at = NOW()
WHERE ncm = '30039056';

-- 3. Verifica resultado final
SELECT
    COUNT(*)                         AS total,
    COUNT(norma_legal_id)            AS com_norma_vinculada,
    COUNT(*) - COUNT(norma_legal_id) AS sem_norma
FROM produtos_monofasicos;

-- Deve retornar: total=952 | com_norma_vinculada=952 | sem_norma=0
