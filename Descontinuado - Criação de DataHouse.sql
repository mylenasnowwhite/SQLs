#Deletar todos os dados da tabela
DELETE FROM dataset.BD_UNION;

#Inserção de dados

INSERT INTO dataset.BD_UNION 
(dt_campanha, 
id_cliente,
nm_ferramenta, 
nm_origemmidia, 
nm_anuncio, 
nm_campanha, 
nm_evento, 
vl_custo, 
qt_impressoes, 
qt_cliques, 
qt_linkcliques, 
qt_conversao, 
qt_engajamento, 
qt_alcance, 
vl_frequencia, 
qt_sessoes, 
qt_sessoes_engajadas, 
qt_rejeicao, 
qt_transacoes, 
vl_receita, 
qt_eventos, 
qt_transacoes_eventos, 
vl_receita_eventos, 
qt_conversoes_eventos, 
qt_conversao_ga, 
qt_conversao_total, 
nm_source, nm_channel)

SELECT /*Facebook Dados*/
	dt_inicio AS dt_campanha,
	id_cliente,
	'Facebook' AS nm_ferramenta,
	'' AS nm_origemmidia,
	nm_anuncio,
	nm_campanha,
	'' AS nm_evento,
	COALESCE(SUM(vl_custo),0) AS vl_custo,
	COALESCE(SUM(qt_impressoes),0) AS qt_impressoes,
	COALESCE(SUM(qt_cliques),0) AS qt_cliques,
	0 AS qt_linkcliques,
	0 AS qt_conversao,
	0 AS qt_engajamento,
	COALESCE(SUM(qt_alcance),0) AS qt_alcance,
	COALESCE(AVG(vl_frequencia),0) AS vl_frequencia,
	0 AS qt_sessoes,
	0 AS qt_sessoes_engajadas,
	0 AS qt_rejeicao,
	0 AS qt_transacoes,
	0 AS vl_receita,
	0 AS qt_eventos,
	0 AS qt_transacoes_eventos,
	0 AS vl_receita_eventos,
	0 AS qt_conversoes_eventos,
	0 AS qt_conversao_ga,
	0 AS qt_conversao_total,
	'' AS SOURCE,
	'' AS CHANNEL
	FROM 
	(SELECT DISTINCT * FROM dataset.metaads_dados WHERE id_cliente > 0) AS metaads_dados
	GROUP BY 
	dt_inicio,
	id_cliente,
	nm_anuncio,
	nm_campanha

UNION ALL

SELECT /*Facebook Actions*/
	dt_inicio AS dt_campanha,
	id_cliente,
	'Facebook Ações' AS nm_ferramenta,
	'' AS nm_origemmidia,
	nm_anuncio,
	nm_campanha,
	'' AS nm_evento,
	0 AS vl_custo,
	0 AS qt_impressoes,
	0 AS qt_cliques,
	if(ds_acao = 'link_click', coalesce(sum(vl_acao),0), 0) AS qt_linkcliques,
	if(ds_acao = 'lead', coalesce(sum(vl_acao),0), 0) AS qt_conversao,
	if(ds_acao = 'post_engagement', coalesce(sum(vl_acao),0), 0) AS qt_engajamento,
	0 AS qt_alcance,
	0 AS vl_frequencia,
	0 AS qt_sessoes,
	0 AS qt_sessoes_engajadas,
	0 AS qt_rejeicao,
	0 AS qt_transacoes,
	0 AS vl_receita,
	0 AS qt_eventos,
	0 AS qt_transacoes_eventos,
	0 AS vl_receita_eventos,
	0 AS qt_conversoes_eventos,
	0 AS qt_conversao_ga,
	0 AS qt_conversao_total,
	'' AS SOURCE,
	'' AS CHANNEL
	FROM
	(
	SELECT DISTINCT 
	a.*,
	b.nm_campanha,
	b.nm_anuncio
	FROM 
	dataset.metaads_acoes AS a
	LEFT JOIN dataset.metaads_dados AS b
	ON 
		 a.id_cliente = b.id_cliente 
	AND a.dt_inicio = b.dt_inicio 
	AND a.id_campanha = b.id_campanha
	) AS metaads_acoes
	GROUP BY 
	dt_inicio,
	id_cliente,
	nm_anuncio,
	nm_campanha

UNION ALL

SELECT /*Google Ads*/
	dt_campanha,
	id_cliente,
	'Google Ads' AS nm_ferramenta,
	'' AS nm_origemmidia,
	nm_anuncio,
	nm_campanha,
	'' AS nm_evento,
	coalesce(sum(vl_custo_ajustado),0) AS vl_custo,
	coalesce(sum(qt_impressoes),0) AS qt_impressoes,
	coalesce(sum(qt_cliques),0) AS qt_cliques,
	0 AS qt_linkcliques,
	coalesce(sum(cast(vl_conversoes AS INT)),0) AS qt_conversao,
	0 AS qt_engajamento,
	0 AS qt_alcance,
	0 AS vl_frequencia,
	0 AS qt_sessoes,
	0 AS qt_sessoes_engajadas,
	0 AS qt_rejeicao,
	0 AS qt_transacoes,
	0 AS vl_receita,
	0 AS qt_eventos,
	0 AS qt_transacoes_eventos,
	0 AS vl_receita_eventos,
	0 AS qt_conversoes_eventos,
	0 AS qt_conversao_ga,
	0 AS qt_conversao_total,
	'' AS SOURCE,
	'' AS CHANNEL
	FROM
	(
		SELECT DISTINCT 
	a.*
	FROM 
	dataset.gads_dados AS a
	WHERE a.id_cliente IN 
	(SELECT b.id_cliente 
	FROM dataset.vw_onemap_clientes b 
	WHERE b.tp_usa_ga4 = 1)
	) AS gads_dados
	GROUP BY 
	dt_campanha,
	id_cliente,
	nm_anuncio,
	nm_campanha 

UNION ALL

SELECT /*Google Universal*/
	dt_sessao as dt_campanha,
	id_cliente,
	'Google Analytics' AS nm_ferramenta,
	nm_origemmidia,
	'' as nm_anuncio,
	nm_campanha,
	'' AS nm_evento,
	coalesce(sum(vl_custo),0) AS vl_custo,
	coalesce(sum(qt_impressoes),0) AS qt_impressoes,
	coalesce(sum(qt_cliques),0) AS qt_cliques,
	0 AS qt_linkcliques,
	0 AS qt_conversao,
	0 AS qt_engajamento,
	0 AS qt_alcance,
	0 AS vl_frequencia,
	coalesce(sum(qt_sessoes),0) AS qt_sessoes,
	0 AS qt_sessoes_engajadas,
	coalesce(sum(qt_bounces),0) AS qt_rejeicao,
	coalesce(sum(qt_transacoes),0) AS qt_transacoes,
	coalesce(sum(vl_receita),0) AS vl_receita,
	0 AS qt_eventos,
	0 AS qt_transacoes_eventos,
	0 AS vl_receita_eventos,
	0 AS qt_conversoes_eventos,
	0 AS qt_conversao_ga,
	0 AS qt_conversao_total,
	'' AS SOURCE,
	'' AS CHANNEL
	FROM
	(
		SELECT DISTINCT 
	a.*
	FROM 
	dataset.gau_campanhas AS a
	WHERE a.id_cliente NOT IN
	(SELECT b.id_cliente 
	FROM dataset.vw_onemap_clientes b 
	WHERE b.tp_usa_ga4 = 1)
	) AS gau_campanhas
	GROUP BY 
	dt_sessao,
	id_cliente,
	nm_origemmidia,
	nm_campanha
	
UNION ALL

SELECT /*GA 4 Sessões*/
	dt_sessao as dt_campanha,
	id_cliente,
	'Google Analytics 4' AS nm_ferramenta,
	nm_origemmidia_sessao as nm_origemmidia,
	'' as nm_anuncio,
	nm_campanha_sessao as nm_campanha,
	'' AS nm_evento,
	0 AS vl_custo,
	0 AS qt_impressoes,
	0 AS qt_cliques,
	0 AS qt_linkcliques,
	0 AS qt_conversao,
	0 AS qt_engajamento,
	0 AS qt_alcance,
	0 AS vl_frequencia,
	coalesce(sum(qt_sessoes),0) AS qt_sessoes,
	coalesce(sum(qt_sessoes_engajadas),0) AS qt_sessoes_engajadas,
	coalesce(sum(qt_sessoes)-SUM(qt_sessoes_engajadas),0) AS qt_rejeicao,
	coalesce(sum(qt_transacoes),0) AS qt_transacoes,
	coalesce(sum(vl_receita),0) AS vl_receita,
	0 AS qt_eventos,
	0 AS qt_transacoes_eventos,
	0 AS vl_receita_eventos,
	0 AS qt_conversoes_eventos,
	0 AS qt_conversao_ga,
	0 AS qt_conversao_total,
	'' AS SOURCE,
	'' AS CHANNEL
	FROM
	(
		SELECT DISTINCT 
	a.*
	FROM 
	dataset.ga4_sessoes AS a
	WHERE a.id_cliente IN
	(SELECT b.id_cliente 
	FROM dataset.vw_onemap_clientes b 
	WHERE b.tp_usa_ga4 = 1)
	) AS ga4_sessoes
	GROUP BY 
	dt_sessao,
	id_cliente,
	nm_origemmidia_sessao,
	nm_campanha_sessao

UNION ALL

SELECT /*GA 4 Eventos*/
	dt_evento AS dt_campanha,
	id_cliente,
	'Google Analytics 4' AS nm_ferramenta,
	nm_origemmidia_sessao as nm_origemmidia,
	'' as nm_anuncio,
	nm_campanha_sessao as nm_campanha,
	nm_evento,
	0 AS vl_custo,
	0 AS qt_impressoes,
	0 AS qt_cliques,
	0 AS qt_linkcliques,
	0 AS qt_conversao,
	0 AS qt_engajamento,
	0 AS qt_alcance,
	0 AS vl_frequencia,
	0 AS qt_sessoes,
	0 AS qt_sessoes_engajadas,
	0 AS qt_rejeicao,
	0 AS qt_transacoes,
	0 AS vl_receita,
	coalesce(sum(qt_eventos),0) AS qt_eventos,
	0 AS qt_transacoes_eventos,
	0 AS vl_receita_eventos,
	0 AS qt_conversoes_eventos,
	coalesce(sum(qt_conversao_ga),0) AS qt_conversao_ga,
	0 AS qt_conversao_total,
	'' AS SOURCE,
	'' AS CHANNEL
	FROM
	(
	SELECT DISTINCT 
	a.*, 
	b.ds_eventos_leads,
	if(a.nm_evento REGEXP b.ds_eventos_leads, COALESCE(SUM(a.qt_eventos),0),0) AS qt_conversao_ga 
	FROM 
	dataset.ga4_eventos AS a
	LEFT JOIN dataset.vw_onemap_clientes AS b
	ON a.id_cliente = b.id_cliente
	WHERE a.id_cliente IN
	(SELECT b.id_cliente 
	FROM dataset.vw_onemap_clientes b 
	WHERE b.tp_usa_ga4 = 1)
	GROUP BY 
	a.dt_evento,
	a.id_cliente,
	a.nm_evento,
	a.nm_origemmidia_sessao,
	a.nm_campanha_sessao,
	b.ds_eventos_leads
	) AS ga4_eventos
	WHERE 
 	qt_conversao_ga > 0
	GROUP BY 
	dt_evento,
	id_cliente,
	nm_origemmidia_sessao,
	nm_campanha_sessao
	
UNION ALL 
	
SELECT /*Bing Dados*/
	dt_campanha,
	id_cliente,
	'Bing' AS nm_ferramenta,
	'' AS nm_origemmidia,
	'' as nm_anuncio,
	nm_campanha,
	'' AS nm_evento,
	COALESCE(SUM(vl_custo),0) AS vl_custo,
	COALESCE(SUM(qt_impressoes),0) AS qt_impressoes,
	COALESCE(SUM(qt_cliques),0) AS qt_cliques,
	0 AS qt_linkcliques,
	0 AS qt_conversao,
	0 AS qt_engajamento,
	0 AS qt_alcance,
	0 AS vl_frequencia,
	0 AS qt_sessoes,
	0 AS qt_sessoes_engajadas,
	0 AS qt_rejeicao,
	0 AS qt_transacoes,
	0 AS vl_receita,
	0 AS qt_eventos,
	0 AS qt_transacoes_eventos,
	0 AS vl_receita_eventos,
	0 AS qt_conversoes_eventos,
	0 AS qt_conversao_ga,
	0 AS qt_conversao_total,
	'' AS SOURCE,
	'' AS CHANNEL
	FROM 
	(SELECT DISTINCT * FROM dataset.bingads_dados) AS bingads_dados
	GROUP BY 
	dt_campanha,
	id_cliente,
	nm_campanha
	
UNION ALL 

SELECT /*Blue Dados*/
	dt_campanha,
	id_cliente,
	'Blue' AS nm_ferramenta,
	'' AS nm_origemmidia,
	'' as nm_anuncio,
	nm_campanha,
	'' AS nm_evento,
	COALESCE(SUM(vl_custo),0) AS vl_custo,
	COALESCE(SUM(qt_impressoes),0) AS qt_impressoes,
	COALESCE(SUM(qt_cliques),0) AS qt_cliques,
	0 AS qt_linkcliques,
	COALESCE(SUM(qt_conversoes),0) AS qt_conversao,
	0 AS qt_engajamento,
	0 AS qt_alcance,
	0 AS vl_frequencia,
	0 AS qt_sessoes,
	0 AS qt_sessoes_engajadas,
	0 AS qt_rejeicao,
	0 AS qt_transacoes,
	0 AS vl_receita,
	0 AS qt_eventos,
	0 AS qt_transacoes_eventos,
	0 AS vl_receita_eventos,
	0 AS qt_conversoes_eventos,
	0 AS qt_conversao_ga,
	0 AS qt_conversao_total,
	'' AS SOURCE,
	'' AS CHANNEL
	FROM 
	(SELECT DISTINCT * FROM dataset.blueads_dados) AS blueads_dados
	GROUP BY 
	dt_campanha,
	id_cliente,
	nm_campanha
	
UNION ALL 

SELECT /*Criteo Dados*/
	dt_campanha,
	id_cliente,
	'Criteo' AS nm_ferramenta,
	'' AS nm_origemmidia,
	'' as nm_anuncio,
	nm_conjuntoanuncio,
	'' AS nm_evento,
	COALESCE(SUM(vl_custo),0) AS vl_custo,
	COALESCE(SUM(qt_display),0) AS qt_impressoes,
	COALESCE(SUM(qt_cliques),0) AS qt_cliques,
	0 AS qt_linkcliques,
	0 AS qt_conversao,
	0 AS qt_engajamento,
	0 AS qt_alcance,
	0 AS vl_frequencia,
	0 AS qt_sessoes,
	0 AS qt_sessoes_engajadas,
	0 AS qt_rejeicao,
	0 AS qt_transacoes,
	0 AS vl_receita,
	0 AS qt_eventos,
	0 AS qt_transacoes_eventos,
	0 AS vl_receita_eventos,
	0 AS qt_conversoes_eventos,
	0 AS qt_conversao_ga,
	0 AS qt_conversao_total,
	'' AS SOURCE,
	'' AS CHANNEL
	FROM 
	(SELECT DISTINCT * FROM dataset.criteoads_dados) AS criteoads_dados
	GROUP BY 
	dt_campanha,
	id_cliente,
	nm_conjuntoanuncio

UNION ALL 

SELECT /*linkedin Dados*/
	dt_campanha,
	id_cliente,
	'LinkedIn' AS nm_ferramenta,
	'' AS nm_origemmidia,
	'' as nm_anuncio,
	nm_campanha,
	'' AS nm_evento,
	COALESCE(SUM(vl_custo),0) AS vl_custo,
	COALESCE(SUM(qt_impressoes),0) AS qt_impressoes,
	COALESCE(SUM(qt_cliques),0) AS qt_cliques,
	0 AS qt_linkcliques,
	COALESCE(SUM(qt_conversoes),0) AS qt_conversao,
	0 AS qt_engajamento,
	0 AS qt_alcance,
	0 AS vl_frequencia,
	0 AS qt_sessoes,
	0 AS qt_sessoes_engajadas,
	0 AS qt_rejeicao,
	0 AS qt_transacoes,
	0 AS vl_receita,
	0 AS qt_eventos,
	0 AS qt_transacoes_eventos,
	0 AS vl_receita_eventos,
	0 AS qt_conversoes_eventos,
	0 AS qt_conversao_ga,
	0 AS qt_conversao_total,
	'' AS SOURCE,
	'' AS CHANNEL
	FROM 
	(SELECT DISTINCT * FROM dataset.linkedinads_dados) AS linkedinads_dados
	GROUP BY 
	dt_campanha,
	id_cliente,
	nm_campanha
	
UNION ALL 

SELECT /*Pinterest Dados*/
	dt_campanha,
	id_cliente,
	'Pinterest' AS nm_ferramenta,
	'' AS nm_origemmidia,
	'' as nm_anuncio,
	nm_campanha,
	'' AS nm_evento,
	COALESCE(SUM(vl_custo),0) AS vl_custo,
	COALESCE(SUM(qt_impressoes),0) AS qt_impressoes,
	COALESCE(SUM(qt_cliques),0) AS qt_cliques,
	0 AS qt_linkcliques,
	COALESCE(SUM(qt_conversoes),0) AS qt_conversao,
	COALESCE(SUM(qt_engajamento),0) AS qt_engajamento,
	0 AS qt_alcance,
	0 AS vl_frequencia,
	0 AS qt_sessoes,
	0 AS qt_sessoes_engajadas,
	0 AS qt_rejeicao,
	0 AS qt_transacoes,
	0 AS vl_receita,
	0 AS qt_eventos,
	0 AS qt_transacoes_eventos,
	0 AS vl_receita_eventos,
	0 AS qt_conversoes_eventos,
	0 AS qt_conversao_ga,
	0 AS qt_conversao_total,
	'' AS SOURCE,
	'' AS CHANNEL
	FROM 
	(SELECT DISTINCT * FROM dataset.pinterestads_dados) AS pinterestads_dados
	GROUP BY 
	dt_campanha,
	id_cliente,
	nm_campanha

UNION ALL 

SELECT /*voxus Dados*/
	dt_conversao,
	id_cliente,
	'Voxus' AS nm_ferramenta,
	'' AS nm_origemmidia,
	'' as nm_anuncio,
	'' as nm_campanha,
	'' AS nm_evento,
	COALESCE(SUM(vl_custo),0) AS vl_custo,
	COALESCE(SUM(qt_impressoes),0) AS qt_impressoes,
	0 AS qt_cliques,
	0 AS qt_linkcliques,
	COALESCE(SUM(qt_conversoes),0) AS qt_conversao,
	0 AS qt_engajamento,
	0 AS qt_alcance,
	0 AS vl_frequencia,
	0 AS qt_sessoes,
	0 AS qt_sessoes_engajadas,
	0 AS qt_rejeicao,
	0 AS qt_transacoes,
	0 AS vl_receita,
	0 AS qt_eventos,
	0 AS qt_transacoes_eventos,
	0 AS vl_receita_eventos,
	0 AS qt_conversoes_eventos,
	0 AS qt_conversao_ga,
	0 AS qt_conversoes_totais,
	'' AS SOURCE,
	'' AS CHANNEL
	FROM 
	(SELECT DISTINCT * FROM dataset.voxusads_dados) AS voxusads_dados
	GROUP BY 
	dt_conversao,
	id_cliente
	

UNION ALL 

SELECT /*tiktok Dados*/
	dt_campanha,
	id_cliente,
	'TikTok' AS nm_ferramenta,
	'' AS nm_origemmidia,
	'' as nm_anuncio,
	nm_campanha,
	'' AS nm_evento,
	COALESCE(SUM(vl_custo),0) AS vl_custo,
	COALESCE(SUM(qt_impressoes),0) AS qt_impressoes,
	COALESCE(SUM(qt_cliques),0) AS qt_cliques,
	0 AS qt_linkcliques,
	0 AS qt_conversao,
	0 AS qt_engajamento,
	COALESCE(SUM(qt_alcance),0) AS qt_alcance,
	COALESCE(AVG(vl_frequencia),0) AS vl_frequencia,
	0 AS qt_sessoes,
	0 AS qt_sessoes_engajadas,
	0 AS qt_rejeicao,
	0 AS qt_transacoes,
	0 AS vl_receita,
	0 AS qt_eventos,
	0 AS qt_transacoes_eventos,
	0 AS vl_receita_eventos,
	0 AS qt_conversoes_eventos,
	0 AS qt_conversao_ga,
	0 AS qt_conversao_total,
	'' AS SOURCE,
	'' AS CHANNEL
	FROM 
	(SELECT DISTINCT * FROM dataset.tiktokads_dados) AS tiktokads_dados
	GROUP BY 
	dt_campanha,
	id_cliente,
	nm_campanha
	;
#Finalização da Inserção
#Inicio da soma dos leads
UPDATE dataset.BD_UNION 
	SET qt_conversao_total = qt_conversao_ga+qt_conversao
	WHERE id_cliente > -1;
#fim da soma dos leads
#inicio das informações por regras de SOURCE 
UPDATE dataset.BD_UNION
	SET nm_source =
    CASE 
        WHEN lower(SUBSTRING_INDEX(nm_origemmidia, '/', 1)) like ('%facebook%') OR lower(nm_origemmidia) like ('fb /%') OR lower(nm_origemmidia) like ('meta /%') OR lower(nm_ferramenta) LIKE '%facebook%'
            THEN 'Facebook' 
        WHEN lower(SUBSTRING_INDEX(nm_origemmidia, '/', 1)) like ('%criteo%') OR nm_ferramenta = 'Criteo'
            THEN 'Criteo'
        WHEN lower(SUBSTRING_INDEX(nm_origemmidia, '/', 1)) like ('%voxus%') OR nm_ferramenta = 'Voxus'
            THEN 'Voxus'
        WHEN lower(SUBSTRING_INDEX(nm_origemmidia, '/', 1)) like ('%blue%') OR nm_ferramenta = 'Blue'
            THEN 'Blue'
        WHEN lower(SUBSTRING_INDEX(nm_origemmidia, '/', 1)) like ('%pinterest%') OR nm_ferramenta = 'Pinterest'
            THEN 'Pinterest'
        WHEN lower(SUBSTRING_INDEX(nm_origemmidia, '/', 1)) like ('%linkedin%') OR nm_ferramenta = 'LinkedIn'
            THEN 'LinkedIn'
        WHEN lower(SUBSTRING_INDEX(nm_origemmidia, '/', 1)) like ('%bing%') OR nm_ferramenta = 'Bing'
            THEN 'Bing'
        WHEN lower(SUBSTRING_INDEX(nm_origemmidia, '/', 1)) like ('%tiktok%') OR nm_ferramenta = 'TikTok'
            THEN 'TikTok'
        WHEN lower(SUBSTRING_INDEX(nm_origemmidia, '/', 1)) like ('%google%') OR nm_ferramenta = 'Google Ads'
            THEN 'Google'
        ELSE 'Outros'
    END
	WHERE nm_source = '';
#Fim das informações por regras de SOURCE 
#Inicio das informações por regra de Channel
UPDATE dataset.BD_UNION
	SET nm_channel = 'Pago'
	WHERE nm_origemmidia = '' AND LOWER(nm_ferramenta) NOT REGEXP 'analytics';
UPDATE dataset.BD_UNION
	SET nm_channel = 'Outros'
	WHERE nm_origemmidia NOT REGEXP '/';
UPDATE dataset.BD_UNION
	SET nm_channel =
    CASE 
        WHEN nm_origemmidia = '' 
		  		THEN 'Pago'
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
WHERE nm_channel = '';
#Inicio das informações por regra de Channel

	
		
	

	