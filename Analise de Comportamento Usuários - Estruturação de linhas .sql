Select * from (
--EVENTOS DO FLUXO fluxo
WITH Eventos_Fluxo_ondeencontrar as (
Select  
datetime(timestamp_micros(event_timestamp), 'America/Sao_Paulo') as dt_evento,
(SELECT value.int_value FROM UNNEST(event_params) AS event_param  WHERE event_param.key = 'ga_session_id') as id_sessao,
REGEXP_REPLACE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'etapa'), r'[0-9:]', '') as nm_etapa,
geo.region as nm_estado,
geo.city as nm_cidade,
geo.country as nm_pais,
Case 
/*  when regexp_contains((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'etapa'),'resultado|interesse') then (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'local')*/
  when (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'etapa') like '%interesse%' and  
  regexp_contains( lower((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'valor_etapa')),'ins') 
  then CONCAT((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'valor_etapa'),"_",(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'local_instalacao'))
  when (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'etapa') like '%99%' then "Sem Resultado"
  when (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'etapa') like '%resultado%' and (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'etapa') not like '%99%' then "Com Resultado"
Else (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'valor_etapa')
end as valores
FROM
  `dataset` a
   where lower(event_name) like '%fluxo%'
and 'fluxo' in (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') 
and (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'etapa') is not null
and datetime(timestamp_micros(event_timestamp), 'America/Sao_Paulo') > '2024-03-04'
group by 1, 2, 3, 4, 5, 6, 7
order by id_sessao asc, dt_evento asc
),
--PARTE QUE INFORMA O NÚMERO DE FORMULARIOS PREENXIDOS
  Fluxo_geral as (
--Id da sessão que iniciaram o fluxo
Select  distinct
(SELECT value.int_value FROM UNNEST(event_params) AS event_param  WHERE event_param.key = 'ga_session_id') as id_sessao,
(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'etapa') as nm_etapa,
datetime(timestamp_micros(event_timestamp), 'America/Sao_Paulo') as dt_evento
FROM
  `dataset` 
  where lower(event_name) like '%fluxo%'
and 'fluxo' in (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') 
and (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'etapa') is not null
and datetime(timestamp_micros(event_timestamp), 'America/Sao_Paulo') > '2024-03-04'
),
  Fluxo_finalizado as (
--Id da sessão que chegaram no final do fluxo em alguma vez
Select  distinct
(SELECT value.int_value FROM UNNEST(event_params) AS event_param  WHERE event_param.key = 'ga_session_id') as id_sessao
FROM
  `dataset` 
  where lower(event_name) like '%fluxo%'
and 'fluxo' in (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') 
and (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'etapa') is not null
and (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'etapa') like "%resultado%"
and datetime(timestamp_micros(event_timestamp), 'America/Sao_Paulo') > '2024-03-04'
),
  Fluxo_ultimaacao as (
-- Id da sessão que não finalizaram o fluxo em nenhuma vez, mais o periodo do final do fluxo.
SELECT id_sessao, max(nm_etapa) as nm_etapa, max(dt_evento) as dt_evento
FROM Fluxo_geral
WHERE id_sessao NOT IN (SELECT id_sessao FROM Fluxo_finalizado)
group by 1

-- Id da sessão que finalizou o fluxo independente das vezes, mais o periodo do final do fluxo.
union all 
Select  distinct
(SELECT value.int_value FROM UNNEST(event_params) AS event_param  WHERE event_param.key = 'ga_session_id') as id_sessao,
(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'etapa') as nm_etapa,
datetime(timestamp_micros(event_timestamp), 'America/Sao_Paulo') as dt_evento
FROM
  `dataset` 
  where lower(event_name) like '%fluxo%'
and 'fluxo' in (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') 
and (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'etapa') is not null
and (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'etapa') like "%resultado%"
and datetime(timestamp_micros(event_timestamp), 'America/Sao_Paulo') > '2024-03-04'
group by 1, 2, 3

--Id da sessão que desistiu de finalizar o fluxo depois de repetir o fluxo mais de uma vez
union all

SELECT id_sessao, max(nm_etapa) as nm_etapa, max(dt_evento) as dt_evento
FROM Fluxo_geral
WHERE id_sessao IN (SELECT id_sessao FROM Fluxo_finalizado)
group by 1
order by 1 asc, 3 asc
  ),
  Formulario_preenchido as (
  Select a.*, b.qtd_formulario from Eventos_Fluxo_ondeencontrar a left join (Select *,ROW_NUMBER() OVER (PARTITION BY  id_sessao ORDER BY min(dt_evento)) as qtd_formulario,
    from (select distinct * from Fluxo_ultimaacao)
    group by 1,2,3
    order by 1 asc, 3 asc) b on a.id_sessao = b.id_sessao and a.dt_evento <= b.dt_evento
    order by a.dt_evento asc
  ),
  Evento_por_formulario as (
    Select dt_evento, id_sessao, nm_etapa, nm_estado, nm_cidade, nm_pais, min(qtd_formulario) as qtd_formulario,valores
    from Formulario_preenchido
    group by dt_evento, id_sessao, nm_etapa, nm_estado, nm_cidade, nm_pais,valores
    order by 1 asc),
  Evento_max_por_formulario as (
    Select max(dt_evento) as dt_evento, id_sessao, nm_etapa, nm_estado, nm_cidade, nm_pais,qtd_formulario,valores
    from Evento_por_formulario
    group by id_sessao, nm_etapa, nm_estado, nm_cidade, nm_pais,qtd_formulario,valores
    order by 2 asc, 1 asc
  ),
    BQD_BRUTOS as (
    Select b.dt_session_start as dt_session_start, a.id_sessao, a.nm_etapa, a.nm_estado, a.nm_cidade, a.qtd_formulario,a.valores
    From Evento_max_por_formulario a 
    left join (
      select 
      (SELECT value.int_value FROM UNNEST(event_params) AS event_param  WHERE event_param.key = 'ga_session_id') as id_sessao,
      min(datetime(timestamp_micros(event_timestamp), 'America/Sao_Paulo')) as dt_session_start
      FROM
      `dataset` 
      where 
      lower(event_name) like '%session_start%' 
      group by 
      1
    ) 
    b on a.id_sessao = b.id_sessao 
    order by 3 asc, 2 asc
    ),
-- Dados quando o usuário clica no botão onde comprar
BOTAO_TRATADO as (
  with BOTAO_ as (
  Select distinct
  datetime(timestamp_micros(event_timestamp), 'America/Sao_Paulo') as dt_evento,
  (SELECT value.int_value FROM UNNEST(event_params) AS event_param  WHERE event_param.key = 'ga_session_id') as id_sessao,
  (SELECT value.string_value FROM UNNEST(event_params) AS event_param  WHERE event_param.key = 'segmento') as tipo_produto,
  (SELECT value.string_value FROM UNNEST(event_params) AS event_param  WHERE event_param.key = 'categoria_produto') as categoria,
  (SELECT value.string_value FROM UNNEST(event_params) AS event_param  WHERE event_param.key = 'subcategoria') as subcategoria
  FROM
    `dataset` 
    where regexp_contains(lower(event_name),'nomedoevento|nomedoevento') and event_name not like 'nomedoevento'
    and datetime(timestamp_micros(event_timestamp), 'America/Sao_Paulo') > '2024-03-04'
  order by id_sessao asc, dt_evento asc
  )
  select 
  b.dt_session_start,
  a.id_sessao,
  a.tipo_produto,
  a.categoria,
  a.subcategoria,
  ROW_NUMBER() OVER (PARTITION BY  a.id_sessao ORDER BY a.dt_evento) as qtd_formulario
  FROM BOTAO a
  left join (
        select distinct
        (SELECT value.int_value FROM UNNEST(event_params) AS event_param  WHERE event_param.key = 'ga_session_id') as id_sessao,
        min(datetime(timestamp_micros(event_timestamp), 'America/Sao_Paulo')) as dt_session_start
        FROM
        `dataset` 
        where 
        lower(event_name) like '%session_start%' 
        group by 
        1
      ) b on a.id_sessao = b.id_sessao
  where 
  a.tipo_produto is not null 
  and a.categoria is not null 
  and a.subcategoria is not null 
  and dt_session_start is not null 
  and dt_session_start >= '2024-03-04'
  order by a.id_sessao asc, b.dt_session_start asc),
-- DEFINIÇÃO DE ORIGEM DO FLUXO DO PREENCHIMENTO DO FORMULARIO
BD_FLUXO_BOTAO AS (
  SELECT
  dt_session_start,
  id_sessao,
  qtd_formulario,
  MAX(if(nm_cidade='','não captado',nm_cidade)) as nm_cidade,
  MAX(ifnull(replace(nm_estado,'State of ',""),'não captado')) AS nm_estado,
  ifnull(MAX(CASE WHEN nm_etapa = 'localizacao' THEN valores END),MAX(concat(if(nm_cidade='','não captado',nm_cidade),' - ', ifnull(replace(nm_estado,'State of ',""),'não captado')))) AS localizacao,
  ifnull(MAX(CASE WHEN nm_etapa = 'tipo-produto' THEN valores END),'') AS tipo_produto,
  ifnull(MAX(CASE WHEN nm_etapa = 'categoria' THEN valores END),'') AS categoria,
  ifnull(MAX(CASE WHEN nm_etapa = 'subcategoria' THEN valores END),'') AS subcategoria,
  ifnull(MAX(CASE WHEN nm_etapa = 'interesse' THEN valores END),'') AS interesse,
  ifnull(MAX(CASE WHEN nm_etapa = 'resultado' THEN valores END),'') AS resultado,
  if(id_sessao in (select distinct id_sessao from BOTAO_ONDE_COMPRAR_TRATADO ),'Fluxo Botão','Fluxo Padrão') as caminho
  FROM BQD_BRUTOS
  WHERE dt_session_start is not null
  GROUP BY dt_session_start, id_sessao, qtd_formulario
  ORDER BY id_sessao,dt_session_start,qtd_formulario
)

SELECT 
 a.dt_session_start,
 a.id_sessao,
 a.qtd_formulario,
 a.nm_cidade,
 a.nm_estado,
 a.localizacao,
 if(a.tipo_produto = '', if(caminho = 'Botão', b.tipo_produto,a.tipo_produto),a.tipo_produto) as tipo_produto,
 if(a.categoria = '', if(caminho = 'Botão', b.categoria,a.categoria),a.categoria) as categoria,
 if(a.subcategoria = '', if(caminho = 'Botão', b.subcategoria,a.subcategoria),a.subcategoria) as subcategoria,
 a.interesse,
 a.resultado,
 a.caminho
FROM BD_FLUXO_BOTAO a 
left join BOTAO_TRATADO b 
on 
a.dt_session_start = b.dt_session_start 
and a.id_sessao = b.id_sessao
and a.qtd_formulario = b.qtd_formulario
order by id_sessao, dt_session_start, qtd_formulario
)
