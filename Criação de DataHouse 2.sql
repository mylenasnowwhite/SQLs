BEGIN
TRUNCATE TABLE dataset.tabela;
INSERT INTO dataset.tabela (
   dt_campanha,
	id_cliente,
	nm_ferramenta,
	nm_anuncio,
	nm_campanha,
	vl_custo,
	qt_impressoes,
	qt_cliques,
	qt_alcance,
	vl_frequencia,
	nm_source,
	nm_channel
)
SELECT /*Facebook Dados*/
	dt_inicio AS dt_campanha,
	id_cliente,
	'Facebook' AS nm_ferramenta,
	nm_anuncio,
	nm_campanha,
	vl_custo,
	qt_impressoes AS qt_impressoes,
	qt_cliques AS qt_cliques,
	qt_alcance AS qt_alcance,
	vl_frequencia AS vl_frequencia,
	'Facebook' AS nm_source,
	'Pago' AS nm_channel
FROM dataset.metaads_dados  AS metaads_dados;
/*-----------------------------------------------------*/
INSERT INTO dataset.tabela (
	 dt_campanha,
	 id_cliente,
	 nm_ferramenta,
	 nm_campanha,
	 qt_linkcliques,
	 qt_conversao,
	 qt_engajamento,
	 nm_source,
	 nm_channel
	
	 )
SELECT /*Facebook Actions*/
	dt_inicio AS dt_campanha,
	id_cliente,
	'Facebook' AS nm_ferramenta,
	nm_campanha,
	if(ds_acao = 'link_click', vl_acao,0) AS qt_linkcliques,
	if(ds_acao = 'lead', vl_acao, 0) AS qt_conversao,
	if(ds_acao = 'post_engagement', vl_acao, 0) AS qt_engajamento,
	'Facebook' AS nm_source,
	'Pago' AS nm_channel
FROM	dataset.metaads_acoes;
/*-----------------------------------------------------*/
INSERT INTO dataset.tabela (
    	dt_campanha,
	id_cliente,
	nm_ferramenta,
	nm_anuncio,
	nm_campanha,
	vl_custo,
	qt_impressoes,
	qt_cliques,
	qt_conversao,
	nm_source,
	nm_channel
)
SELECT /*Google Ads*/
	dt_campanha,
	id_cliente,
	'Google Ads' AS nm_ferramenta,
	nm_anuncio,
	nm_campanha,
	vl_custo_ajustado AS vl_custo,
	qt_impressoes AS qt_impressoes,
	qt_cliques AS qt_cliques,
	CAST(vl_conversoes AS INT) AS qt_conversao,
	'Google' AS nm_source,
	'Pago' AS nm_channel
FROM dataset.gads_dados;
/*-----------------------------------------------------*/
INSERT INTO dataset.tabela (
	dt_campanha,
	id_cliente,
	nm_ferramenta,
	nm_origemmidia,
	nm_campanha,
	qt_sessoes,
	qt_sessoes_engajadas,
	qt_rejeicao,
	qt_transacoes,
	vl_receita
)
SELECT
	dt_sessao AS dt_campanha,
	id_cliente,
	'Google Analytics 4' AS nm_ferramenta,
	nm_origemmidia_sessao AS nm_origemmidia,
	nm_campanha_sessao AS nm_campanha,
	qt_sessoes,
	qt_sessoes_engajadas,
	qt_sessoes - qt_sessoes_engajadas AS qt_rejeicao,
	qt_transacoes,
	vl_receita
FROM dataset.ga4_sessoes;
/*-----------------------------------------------------*/
INSERT INTO dataset.tabela (
	dt_campanha,
	id_cliente,
	nm_ferramenta,
	nm_origemmidia,
	nm_campanha,
	nm_evento,
   	qt_eventos,
	qt_transacoes_eventos,
	qt_conversao_eventos,
	vl_receita_eventos
)
SELECT /*GA 4 Eventos*/
	dt_evento AS dt_campanha,
	id_cliente,
	'Google Analytics 4' AS nm_ferramenta,
	nm_origemmidia_sessao as nm_origemmidia,
	nm_campanha_sessao as nm_campanha,
	nm_evento,
	qt_eventos,
	qt_transacoes,
	qt_conversoes,
	vl_receita
FROM dataset.ga4_eventos;
/*-----------------------------------------------------*/
INSERT INTO dataset.tabela (
dt_campanha,
id_cliente,
nm_evento,
nm_origemmidia,
nm_campanha,
qt_conversao_ga
)
SELECT
    a.dt_evento AS dt_campanha,
    a.id_cliente,
    a.nm_evento,
    a.nm_origemmidia_sessao AS nm_origemmidia,
    a.nm_campanha_sessao AS nm_campanha,
    a.qt_eventos AS qt_conversao_ga
FROM
    dataset.ga4_eventos a
LEFT JOIN
    dataset.vw_onemap_clientes b
    ON a.id_cliente = b.id_cliente
WHERE
    b.ds_eventos_leads IS NOT NULL
    AND b.ds_eventos_leads <> ''
    AND a.nm_evento REGEXP b.ds_eventos_leads;
	 /*-----------------------------------------------------*/
	 INSERT INTO dataset.tabela (
    	dt_campanha,
	id_cliente,
	nm_ferramenta,
	nm_campanha,
	vl_custo,
	qt_impressoes,
	qt_cliques,
	nm_source,
	nm_channel
)
SELECT /*Bing Dados*/
	dt_campanha,
	id_cliente,
	'Bing' AS nm_ferramenta,
	nm_campanha,
	vl_custo AS vl_custo,
	qt_impressoes AS qt_impressoes,
	qt_cliques AS qt_cliques,
	'Bing' AS nm_source,
	'Pago' AS nm_channel
FROM dataset.bingads_dados;
/*-----------------------------------------------------*/
INSERT INTO dataset.tabela (
    	dt_campanha,
	id_cliente,
	nm_ferramenta,
	nm_campanha,
	vl_custo,
	qt_impressoes,
	qt_cliques,
	qt_conversao,
	nm_source,
	nm_channel
)
SELECT /*Blue Dados*/
	dt_campanha,
	id_cliente,
	'Blue' AS nm_ferramenta,
	nm_campanha,
	vl_custo,
	qt_impressoes,
	qt_cliques,
	qt_conversoes AS qt_conversao,
	'Blue' AS nm_source,
	'Pago' AS nm_channel
FROM dataset.blueads_dados;
/*-----------------------------------------------------*/
INSERT INTO dataset.tabela (
    	dt_campanha,
	id_cliente,
	nm_ferramenta,
	nm_campanha,
	vl_custo,
	qt_impressoes,
	qt_cliques,
	nm_source,
	nm_channel
)
SELECT /*Criteo Dados*/
	dt_campanha,
	id_cliente,
	'Criteo' AS nm_ferramenta,
	nm_conjuntoanuncio AS nm_campanha,
	vl_custo,
	qt_display AS qt_impressoes,
	qt_cliques,
	'Criteo' AS nm_source,
	'Pago' AS nm_channel
FROM dataset.criteoads_dados;
/*-----------------------------------------------------*/
INSERT INTO dataset.tabela (
    	dt_campanha,
	id_cliente,
	nm_ferramenta,
	nm_campanha,
	vl_custo,
	qt_impressoes,
	qt_cliques,
	qt_conversao,
	nm_source,
	nm_channel
)
SELECT /*linkedin Dados*/
	dt_campanha,
	id_cliente,
	'LinkedIn' AS nm_ferramenta,
	nm_campanha,
	vl_custo,
	qt_impressoes,
	qt_cliques,
	qt_conversoes AS qt_conversao,
	'LinkedIn' AS nm_source,
	'Pago' AS nm_channel
FROM  dataset.linkedinads_dados;
/*-----------------------------------------------------*/
INSERT INTO dataset.tabela (
    	dt_campanha,
	id_cliente,
	nm_ferramenta,
	nm_campanha,
	vl_custo,
	qt_impressoes,
	qt_cliques,
	qt_conversao,
	qt_engajamento,
	nm_source,
	nm_channel
)
SELECT /*Pinterest Dados*/
	dt_campanha,
	id_cliente,
	'Pinterest' AS nm_ferramenta,
	nm_campanha,
	vl_custo,
	qt_impressoes,
	qt_cliques,
	qt_conversoes AS qt_conversao,
	qt_engajamento,
	'Pinterest' AS nm_source,
	'Pago' AS nm_channel
FROM dataset.pinterestads_dados;
/*-----------------------------------------------------*/
INSERT INTO dataset.tabela (
    	dt_campanha,
	id_cliente,
	nm_ferramenta,
	vl_custo,
	qt_impressoes,
	qt_conversao,
	nm_source,
	nm_channel
)
SELECT /*voxus Dados*/
	dt_conversao AS dt_campanha,
	id_cliente,
	'Voxus' AS nm_ferramenta,
	vl_custo,
	qt_impressoes,
	qt_conversoes AS qt_conversao,
	'Voxus' AS nm_source,
	'Pago' AS nm_channel
FROM  dataset.voxusads_dados;
/*-----------------------------------------------------*/
INSERT INTO dataset.tabela (
    	dt_campanha,
	id_cliente,
	nm_ferramenta,
	nm_campanha,
	vl_custo,
	qt_impressoes,
	qt_cliques,
	qt_alcance,
	vl_frequencia,
	nm_source,
	nm_channel
)
SELECT /*tiktok Dados*/
	dt_campanha,
	id_cliente,
	'TikTok' AS nm_ferramenta,
	nm_campanha,
	vl_custo,
	qt_impressoes,
	qt_cliques,
	qt_alcance,
	vl_frequencia,
	'TikTok' AS nm_source,
	'Pago' AS nm_channel
FROM dataset.tiktokads_dados;
/*-----------------------------------------------------*/
UPDATE dataset.tabela
	SET qt_conversao_total = qt_conversao_ga+qt_conversao
	WHERE id_cliente > -1;
#fim da soma dos leads
#inicio das informações por regras de nm_source
UPDATE dataset.tabela
	SET nm_source =
    CASE
        WHEN lower(SUBSTRING_INDEX(nm_origemmidia, '/', 1)) like ('%facebook%') OR lower(nm_origemmidia) like ('fb /%') OR lower(nm_origemmidia) like ('meta /%')
            THEN 'Facebook'
        WHEN lower(SUBSTRING_INDEX(nm_origemmidia, '/', 1)) like ('%criteo%')
            THEN 'Criteo'
        WHEN lower(SUBSTRING_INDEX(nm_origemmidia, '/', 1)) like ('%voxus%')
            THEN 'Voxus'
        WHEN lower(SUBSTRING_INDEX(nm_origemmidia, '/', 1)) like ('%blue%')
            THEN 'Blue'
        WHEN lower(SUBSTRING_INDEX(nm_origemmidia, '/', 1)) like ('%pinterest%')
            THEN 'Pinterest'
        WHEN lower(SUBSTRING_INDEX(nm_origemmidia, '/', 1)) like ('%linkedin%')
            THEN 'LinkedIn'
        WHEN lower(SUBSTRING_INDEX(nm_origemmidia, '/', 1)) like ('%bing%')
            THEN 'Bing'
        WHEN lower(SUBSTRING_INDEX(nm_origemmidia, '/', 1)) like ('%tiktok%')
            THEN 'TikTok'
        WHEN lower(SUBSTRING_INDEX(nm_origemmidia, '/', 1)) like ('%google%')
            THEN 'Google'
        ELSE 'Outros'
    END
	WHERE nm_source = '' OR nm_source IS NULL;
#Fim das informações por regras de nm_source
#Inicio das informações por regra de nm_channel
UPDATE dataset.tabela
	SET nm_channel = 'Outros'
	WHERE nm_origemmidia NOT REGEXP '/';
UPDATE dataset.tabela
	SET nm_channel =
    CASE
		  		
        WHEN nm_origemmidia = '(direct) / (none)'
		  		THEN 'Direto'
        WHEN nm_origemmidia LIKE '%/ referral'
		  		THEN 'Referência'
        WHEN nm_origemmidia LIKE '%/ organic'
		  		THEN 'Orgânico'
        WHEN LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%push%'
		  		THEN 'Push Marketing'
        WHEN LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%sms%' OR
		  		 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', 1)) LIKE '%sms%'
				THEN 'SMS Marketing'
        WHEN LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%mail%' OR
		  		 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%emkt%' OR
				 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) = 'emm' OR
				 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%jornada%' OR
				 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', 1)) LIKE '%email%'
				THEN 'E-mail Marketing'
        WHEN LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%cpc%' OR
		  		 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%cpa%' OR
				 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%cpl%' OR
				 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%cpm%' OR
				 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%ads%' OR
				 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%ppc%' OR
				 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%display%' OR
				 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%retarget%' OR
				 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%shopping%' OR
				 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) = 'dpa' OR
				 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%afiliado%' OR
				 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%affiliate%' OR
				 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%branding%' OR
				 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%paid%' OR
				 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%search%'
				THEN 'Pago'
        WHEN LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%social%' OR
		  		 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%post%' OR
				 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%stories%' OR
				 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%story%' OR
				 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%feed%' OR
				 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%bio%'
				THEN 'Social'
        WHEN vl_custo > 0 THEN 'Pago'
        ELSE 'Outros'
    END
WHERE nm_channel = '' OR nm_channel IS NULL;
END