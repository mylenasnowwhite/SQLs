# VALIDAÇÃO GERAL 
With BQ_FUNCIONALIDADE_APIS as (
with Base_datas as
  (
    SELECT DISTINCT dt_campanha
    FROM `projeto.raw_marketing.googleads_dados`
    union all
    SELECT DISTINCT dt_campanha
    FROM `projeto.raw_marketing.metaads_dados`
    union all
    SELECT DISTINCT dt_campanha
    FROM `projeto.raw_marketing.pinterestads_dados`
    union all
    SELECT DISTINCT dt_campanha
    FROM `projeto.raw_marketing.tiktokads_dados`
    union all
    SELECT DISTINCT dt_campanha
    FROM `projeto.raw_marketing.linkedinads_dados`
    union all
    SELECT DISTINCT dt_campanha
    FROM `dataset`
    union all
    SELECT DISTINCT dt_campanha
    FROM `dataset`
  ) 
#Google Ads
  Select distinct 
    a.dt_campanha,
    ifnull(b.ferramenta,'google ads') as ferramenta,  
    ifnull(b.qtd_conta,0) as qtd_conta_captadas, 
    ifnull(c.qtd_contas_cadastradas, (Select count(distinct id_conta) as qtd_contas_cadastradas from `projeto.raw_marketing.id_contas` where nm_ferramentaads like '%google%')) as qtd_contas_cadastradas,
    ifnull(b.qtd_conta,0)/ifnull(c.qtd_contas_cadastradas, (Select count(distinct id_conta) as qtd_contas_cadastradas from `projeto.raw_marketing.id_contas` where nm_ferramentaads like '%google%')) as pct_captacao, 
    ifnull(d.id_conta_ausentes, "Todos as contas") as id_conta_ausentes
  from Base_datas a
  left join (
      select 
      dt_campanha, 
      count(distinct id_conta) as qtd_conta, 
      'google ads' as ferramenta 
      from `projeto.raw_marketing.googleads_dados` 
      group by 1
    ) b on a.dt_campanha = b.dt_campanha
  left join (
      Select
      regexp_replace(lower(nm_ferramentaads),'ads','') as ferramenta,
      count(distinct id_conta) as qtd_contas_cadastradas,
      from `projeto.raw_marketing.id_contas`
      group by 1
    ) c on regexp_contains(b.ferramenta,c.ferramenta)
  left join (
      Select 
      *,
      'google ads' as ferramenta
      FROM (
        with id_contas_ativas as (
            select id_conta
            from `projeto.raw_marketing.id_contas`
            where nm_ferramentaads like "%google%"
          ),
        dadosdiarios_midiaads as (
            select distinct dt_campanha, id_conta
            from `projeto.raw_marketing.googleads_dados`
          )
        Select 
          d.dt_campanha, 
          string_agg(c.id_conta,"; ") as id_conta_ausentes
          from id_contas_ativas as c
          cross join (
            select distinct dt_campanha
            from dadosdiarios_midiaads
          ) as d
          left join dadosdiarios_midiaads as b
          on c.id_conta = b.id_conta and d.dt_campanha = b.dt_campanha
          where b.id_conta is null
          group by d.dt_campanha
      )
    ) d on a.dt_campanha = d.dt_campanha and b.ferramenta = d.ferramenta

UNION ALL
#Pinterest Ads
  Select distinct 
    a.dt_campanha,
    ifnull(b.ferramenta,'pinterest ads') as ferramenta,  
    ifnull(b.qtd_conta,0) as qtd_conta_captadas, 
    ifnull(c.qtd_contas_cadastradas, (Select count(distinct id_conta) as qtd_contas_cadastradas from `projeto.raw_marketing.id_contas` where nm_ferramentaads like '%pinterest%')) as qtd_contas_cadastradas,
    ifnull(b.qtd_conta,0)/ifnull(c.qtd_contas_cadastradas, (Select count(distinct id_conta) as qtd_contas_cadastradas from `projeto.raw_marketing.id_contas` where nm_ferramentaads like '%pinterest%')) as pct_captacao, 
    ifnull(d.id_conta_ausentes, "Todos as contas") as id_conta_ausentes  
  from Base_datas a
  left join (
      select 
      dt_campanha, 
      count(distinct id_conta) as qtd_conta, 
      'pinterest ads' as ferramenta 
      from `projeto.raw_marketing.pinterestads_dados` 
      group by 1
    ) b on a.dt_campanha = b.dt_campanha
  left join (
      Select
      regexp_replace(lower(nm_ferramentaads),'ads','') as ferramenta,
      count(distinct id_conta) as qtd_contas_cadastradas,
      from `projeto.raw_marketing.id_contas`
      group by 1
    ) c on regexp_contains(b.ferramenta,c.ferramenta)
  left join (
      Select 
      *,
      'pinterest ads' as ferramenta
      FROM (
        with id_contas_ativas as (
            select id_conta
            from `projeto.raw_marketing.id_contas`
            where nm_ferramentaads like "%pinterest%"
          ),
        dadosdiarios_midiaads as (
            select distinct dt_campanha, id_conta
            from `projeto.raw_marketing.pinterestads_dados`
          )
        Select 
          d.dt_campanha, 
          string_agg(c.id_conta,"; ") as id_conta_ausentes
          from id_contas_ativas as c
          cross join (
            select distinct dt_campanha
            from dadosdiarios_midiaads
          ) as d
          left join dadosdiarios_midiaads as b
          on c.id_conta = b.id_conta and d.dt_campanha = b.dt_campanha
          where b.id_conta is null
          group by d.dt_campanha
      )
    ) d on a.dt_campanha = d.dt_campanha and b.ferramenta = d.ferramenta

UNION ALL
#Tiktok Ads
  Select distinct 
    a.dt_campanha,
    ifnull(b.ferramenta,'tiktok ads') as ferramenta,  
    ifnull(b.qtd_conta,0) as qtd_conta_captadas, 
    ifnull(c.qtd_contas_cadastradas, (Select count(distinct id_conta) as qtd_contas_cadastradas from `projeto.raw_marketing.id_contas` where nm_ferramentaads like '%tiktok%')) as qtd_contas_cadastradas,
    ifnull(b.qtd_conta,0)/ifnull(c.qtd_contas_cadastradas, (Select count(distinct id_conta) as qtd_contas_cadastradas from `projeto.raw_marketing.id_contas` where nm_ferramentaads like '%tiktok%')) as pct_captacao, 
    ifnull(d.id_conta_ausentes, "Todos as contas") as id_conta_ausentes
  from Base_datas a
  left join (
      select 
      dt_campanha, 
      count(distinct id_conta) as qtd_conta, 
      'tiktok ads' as ferramenta 
      from `projeto.raw_marketing.tiktokads_dados` 
      group by 1
    ) b on a.dt_campanha = b.dt_campanha
  left join (
      Select
      regexp_replace(lower(nm_ferramentaads),'ads','') as ferramenta,
      count(distinct id_conta) as qtd_contas_cadastradas,
      from `projeto.raw_marketing.id_contas`
      group by 1
    ) c on regexp_contains(b.ferramenta,c.ferramenta)
  left join (
      Select 
      *,
      'tiktok ads' as ferramenta
      FROM (
        with id_contas_ativas as (
            select id_conta
            from `projeto.raw_marketing.id_contas`
            where nm_ferramentaads like "%tiktok%"
          ),
        dadosdiarios_midiaads as (
            select distinct dt_campanha, id_conta
            from `projeto.raw_marketing.tiktokads_dados`
          )
        Select 
          d.dt_campanha, 
          string_agg(c.id_conta,"; ") as id_conta_ausentes
          from id_contas_ativas as c
          cross join (
            select distinct dt_campanha
            from dadosdiarios_midiaads
          ) as d
          left join dadosdiarios_midiaads as b
          on c.id_conta = b.id_conta and d.dt_campanha = b.dt_campanha
          where b.id_conta is null
          group by d.dt_campanha
      )
    ) d on a.dt_campanha = d.dt_campanha and b.ferramenta = d.ferramenta

UNION ALL
#linkedin Ads
  Select distinct 
    a.dt_campanha,
    ifnull(b.ferramenta,'linkedin ads') as ferramenta,  
    ifnull(b.qtd_conta,0) as qtd_conta_captadas, 
    ifnull(c.qtd_contas_cadastradas, (Select count(distinct id_conta) as qtd_contas_cadastradas from `projeto.raw_marketing.id_contas` where nm_ferramentaads like '%linkedin%')) as qtd_contas_cadastradas,
    ifnull(b.qtd_conta,0)/ifnull(c.qtd_contas_cadastradas, (Select count(distinct id_conta) as qtd_contas_cadastradas from `projeto.raw_marketing.id_contas` where nm_ferramentaads like '%linkedin%')) as pct_captacao, 
    ifnull(d.id_conta_ausentes, "Todos as contas") as id_conta_ausentes
  from Base_datas a
  left join (
      select 
      dt_campanha, 
      count(distinct id_conta) as qtd_conta, 
      'linkedin ads' as ferramenta 
      from `projeto.raw_marketing.linkedinads_dados` 
      group by 1
    ) b on a.dt_campanha = b.dt_campanha
  left join (
      Select
      regexp_replace(lower(nm_ferramentaads),'ads','') as ferramenta,
      count(distinct id_conta) as qtd_contas_cadastradas,
      from `projeto.raw_marketing.id_contas`
      group by 1
    ) c on regexp_contains(b.ferramenta,c.ferramenta)
  left join (
      Select 
      *,
      'linkedin ads' as ferramenta
      FROM (
        with id_contas_ativas as (
            select id_conta
            from `projeto.raw_marketing.id_contas`
            where nm_ferramentaads like "%linkedin%"
          ),
        dadosdiarios_midiaads as (
            select distinct dt_campanha, id_conta
            from `projeto.raw_marketing.linkedinads_dados`
          )
        Select 
          d.dt_campanha, 
          string_agg(c.id_conta,"; ") as id_conta_ausentes
          from id_contas_ativas as c
          cross join (
            select distinct dt_campanha
            from dadosdiarios_midiaads
          ) as d
          left join dadosdiarios_midiaads as b
          on c.id_conta = b.id_conta and d.dt_campanha = b.dt_campanha
          where b.id_conta is null
          group by d.dt_campanha
      )
    ) d on a.dt_campanha = d.dt_campanha and b.ferramenta = d.ferramenta

UNION ALL
#meta Ads dados
  Select distinct 
    a.dt_campanha,
    ifnull(b.ferramenta,'meta ads') as ferramenta,  
    ifnull(b.qtd_conta,0) as qtd_conta_captadas, 
    ifnull(c.qtd_contas_cadastradas, (Select count(distinct id_conta) as qtd_contas_cadastradas from `projeto.raw_marketing.id_contas` where nm_ferramentaads like '%meta%')) as qtd_contas_cadastradas,
    ifnull(b.qtd_conta,0)/ifnull(c.qtd_contas_cadastradas, (Select count(distinct id_conta) as qtd_contas_cadastradas from `projeto.raw_marketing.id_contas` where nm_ferramentaads like '%meta%')) as pct_captacao, 
    ifnull(d.id_conta_ausentes, "Todos as contas") as id_conta_ausentes
  from Base_datas a
  left join (
      select 
      dt_campanha, 
      count(distinct id_conta) as qtd_conta, 
      'meta ads' as ferramenta 
      from `projeto.raw_marketing.metaads_dados` 
      group by 1
    ) b on a.dt_campanha = b.dt_campanha
  left join (
      Select
      regexp_replace(lower(nm_ferramentaads),'ads','') as ferramenta,
      count(distinct id_conta) as qtd_contas_cadastradas,
      from `projeto.raw_marketing.id_contas`
      group by 1
    ) c on regexp_contains(b.ferramenta,c.ferramenta)
  left join (
      Select 
      *,
      'meta ads' as ferramenta
      FROM (
        with id_contas_ativas as (
            select id_conta
            from `projeto.raw_marketing.id_contas`
            where nm_ferramentaads like "%meta%"
          ),
        dadosdiarios_midiaads as (
            select distinct dt_campanha, id_conta
            from `projeto.raw_marketing.metaads_dados`
          )
        Select 
          d.dt_campanha, 
          string_agg(c.id_conta,"; ") as id_conta_ausentes
          from id_contas_ativas as c
          cross join (
            select distinct dt_campanha
            from dadosdiarios_midiaads
          ) as d
          left join dadosdiarios_midiaads as b
          on c.id_conta = b.id_conta and d.dt_campanha = b.dt_campanha
          where b.id_conta is null
          group by d.dt_campanha
      )
    ) d on a.dt_campanha = d.dt_campanha and b.ferramenta = d.ferramenta

UNION ALL
#meta Ads acoes
  Select distinct 
    a.dt_campanha,
    ifnull(b.ferramenta,'meta ads acoes') as ferramenta,  
    ifnull(b.qtd_conta,0) as qtd_conta_captadas, 
    ifnull(c.qtd_contas_cadastradas, (Select count(distinct id_conta) as qtd_contas_cadastradas from `projeto.raw_marketing.id_contas` where nm_ferramentaads like '%meta%')) as qtd_contas_cadastradas,
    ifnull(b.qtd_conta,0)/ifnull(c.qtd_contas_cadastradas, (Select count(distinct id_conta) as qtd_contas_cadastradas from `projeto.raw_marketing.id_contas` where nm_ferramentaads like '%meta%')) as pct_captacao, 
    ifnull(d.id_conta_ausentes, "Todos as contas") as id_conta_ausentes 
  from Base_datas a
  left join (
      select 
      dt_campanha, 
      count(distinct id_conta) as qtd_conta, 
      'meta ads acoes' as ferramenta 
      from `projeto.raw_marketing.metaads_acoes` 
      group by 1
    ) b on a.dt_campanha = b.dt_campanha
  left join (
      Select
      regexp_replace(lower(nm_ferramentaads),'ads','') as ferramenta,
      count(distinct id_conta) as qtd_contas_cadastradas,
      from `projeto.raw_marketing.id_contas`
      group by 1
    ) c on regexp_contains(b.ferramenta,c.ferramenta)
  left join (
      Select 
      *,
      'meta ads acoes' as ferramenta
      FROM (
        with id_contas_ativas as (
            select id_conta
            from `projeto.raw_marketing.id_contas`
            where nm_ferramentaads like "%meta%"
          ),
        dadosdiarios_midiaads as (
            select distinct dt_campanha, id_conta
            from `projeto.raw_marketing.metaads_acoes`
          )
        Select 
          d.dt_campanha, 
          string_agg(c.id_conta,"; ") as id_conta_ausentes
          from id_contas_ativas as c
          cross join (
            select distinct dt_campanha
            from dadosdiarios_midiaads
          ) as d
          left join dadosdiarios_midiaads as b
          on c.id_conta = b.id_conta and d.dt_campanha = b.dt_campanha
          where b.id_conta is null
          group by d.dt_campanha
      )
    ) d on a.dt_campanha = d.dt_campanha and b.ferramenta = d.ferramenta

UNION ALL
#ferramenta - Programatica
  Select distinct 
    a.dt_campanha,
    ifnull(b.ferramenta,"ferramenta") as ferramenta,  
    ifnull(b.qtd_conta,0) as qtd_conta_captadas, 
    1 as qtd_contas_cadastradas, 
    ifnull(b.qtd_conta,0)/ 1 as pct_captacao, 
    'conta unica' as id_conta_ausentes
  from Base_datas a
  left join (select distinct dt_campanha, 'ferramenta' as ferramenta, 1 as qtd_conta from `dataset`) b on a.dt_campanha = b.dt_campanha

UNION ALL
#ferramenta - Programatica
  Select distinct 
    a.dt_campanha,
    ifnull(b.ferramenta,"ferramenta") as ferramenta,  
    ifnull(b.qtd_conta,0) as qtd_conta_captadas, 
    1 as qtd_contas_cadastradas, 
    ifnull(b.qtd_conta,0)/ 1 as pct_captacao, 
    'conta unica' as id_conta_ausentes
  from Base_datas a
  left join (select distinct dt_campanha, 'ferramenta' as ferramenta, 1 as qtd_conta from `dataset`) b on a.dt_campanha = b.dt_campanha
)
Select * from BQ_FUNCIONALIDADE_APIS where ferramenta is not null