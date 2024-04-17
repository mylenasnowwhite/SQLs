Select 
    C.date
    ,C.origem
    ,C.Status
    ,ifnull(if(regexp_contains(c.status, "Status: OK") = true,"",B.retroativos ), "Todos os clientes") as id_cliente_retroativo
  from 
    (
        Select distinct data.date as date, data.origem as origem, if(status.status is null, "ATENÇÃO SEM DADOS",status.status) as status from
          (
              SELECT DISTINCT
                  date, 
                  media_class as origem
                FROM (
                  SELECT DISTINCT CAST(date_start AS DATE) AS date FROM `desenvolvimento.fb_dados`
                  UNION ALL SELECT DISTINCT CAST(date_start AS DATE) AS date FROM `desenvolvimento.fb_actions`
                  UNION ALL SELECT DISTINCT CAST(dt_sessao AS DATE) AS date FROM `desenvolvimento.ga_campanhas`
                  UNION ALL SELECT DISTINCT CAST(date AS DATE) AS date FROM `projeto.desenvolvimento.googleads_dados`
                  UNION ALL SELECT DISTINCT CAST(dt_sessao AS DATE) AS date FROM `projeto.desenvolvimento.ga4_sessoes`
                  UNION ALL SELECT DISTINCT CAST(dt_sessao AS DATE) AS date FROM `projeto.desenvolvimento.ga4_eventos`
                  UNION ALL SELECT DISTINCT CAST(data AS DATE) AS date FROM `projeto.desenvolvimento.voxus_dados`
                  UNION ALL SELECT DISTINCT CAST(data AS DATE) AS date FROM `projeto.desenvolvimento.dados_bing_ads`
                  UNION ALL SELECT DISTINCT CAST(data AS DATE) AS date FROM `projeto.desenvolvimento.dados_blue_ads`
                  UNION ALL SELECT DISTINCT CAST(data AS DATE) AS date FROM `projeto.desenvolvimento.dados_pinterest_ads`
                  UNION ALL SELECT DISTINCT CAST(data AS DATE) AS date FROM `projeto.desenvolvimento.tiktok_dados`
                  UNION ALL SELECT DISTINCT CAST(data AS DATE) AS date FROM `projeto.desenvolvimento.dados_linkedin_ads`
                  UNION ALL SELECT DISTINCT CAST(data AS DATE) AS date FROM `projeto.desenvolvimento.dados_criteo_ads`
                ) AS dates
                CROSS JOIN UNNEST(["Facebook dados", "Facebook Actions", "Google Ads", "GAU", "GA4", "Voxus", "Bing", "Blue", "Pinterest", "TikTok", "Linkedin", "Criteo", "GA4 Eventos"]) AS media_class
                ORDER BY date, media_class
          ) data left join 

          (
                select distinct  #Facebook dados
                date_start as Data
                , "Facebook dados" as Origem
              , concat("qtd id_cliente = ",count(distinct id_cliente), 
                        " | qtd account_id = ",count(distinct account_id), " | % de busca = ", (count(distinct account_id)/(Select 
                                            (count(distinct id_facebook)-1) as facebook_ads 
                                            From `desenvolvimento.clientes`))*100 ,  
                        " | Status: ", if(
                                          if(count(distinct account_id)-count(distinct id_cliente)>0
                                            ,"ATENÇÃO ID_CLIENTE"
                                            ,"OK") = "OK", 
                                              if(count(distinct account_id)< (Select 
                                                                                  (count(distinct id_facebook)-1)*0.51 as Meta_ads 
                                                                                  From `desenvolvimento.clientes`)
                                                ," PUXAR RETROATIVOS"
                                                ,"OK")
                                        ,"ATENÇÂO ID_CLIENTE")
                                        ) as Status
                  from `desenvolvimento.fb_dados`
                    group by date_start

              Union All 

              SELECT #Facebook Actions
                a.Data
                , "Facebook Actions"
                , IF(a.Contagem >= d.Contagem, concat("qtd_id_actions: ", a.Contagem," | qtd_id_fb_dados: ", d.contagem," | % de busca = ", 
                ((a.Contagem/(Select (count(distinct id_facebook)-1) as facebook_ads From `desenvolvimento.clientes`))*100) ," | Status: OK"), Concat("CLIENTE A MENOS, qtd_id_fb_actions = ",a.Contagem," e qtd_id_fb_dados = ",d.Contagem," | % de busca = ",((a.Contagem/(Select (count(distinct id_facebook)-1) as facebook_ads From `desenvolvimento.clientes`))*100) )) AS Status_fb_action
                  FROM (
                    SELECT DATE(date_start) AS Data, COUNT(DISTINCT id_cliente) AS Contagem
                    FROM `desenvolvimento.fb_actions`
                    GROUP BY Data
                  ) AS a
                  JOIN (
                    SELECT DATE(date_start) AS Data, COUNT(DISTINCT id_cliente) AS Contagem
                    FROM `desenvolvimento.fb_dados`
                    GROUP BY Data
                  ) AS d ON a.Data = d.Data

              Union All 

              select distinct  #Google Ads
                date as Data
                , "Google Ads"
              , concat("qtd id_cliente = ",count(distinct id_cliente), 
                        " | qtd account_id = ",count(distinct customer_id), " | % de busca = ", (count(distinct customer_id)/(Select 
                                            (count(distinct id_google_ads)-1) as google_ads 
                                            From `desenvolvimento.clientes`))*100 ,  
                        " | Status: ", if(
                                          if(count(distinct customer_id)-count(distinct id_cliente)>0
                                            ,"ATENÇÃO ID_CLIENTE"
                                            ,"OK") = "OK", 
                                              if(count(distinct customer_id)< (Select 
                                                                                  (count(distinct id_google_ads)-1)*0.51 as Google_ads 
                                                                                  From `desenvolvimento.clientes`)
                                                ," PUXAR RETROATIVOS"
                                                ,"OK")
                                        ,"ATENÇÂO ID_CLIENTE")
                                        ) as Status_googleads_dados
                  from `desenvolvimento.googleads_dados`
                    group by date

              Union All 

              select distinct  #Gogole Analytics Universal 
                dt_sessao as Data
                ,"GAU"
              , concat("qtd id_cliente = ",count(distinct id_cliente), 
                        " | qtd account_id = ",count(distinct id_google_view)," | % de busca = ", (count(distinct id_google_view)/(Select 
                                            (count(distinct id_google_conta)-1) as gau_dados 
                                            From `desenvolvimento.clientes`))*100 , 
                        " | Status: ", if(
                                          if(count(distinct id_google_view)-count(distinct id_cliente)>0
                                            ,"ATENÇÃO ID_CLIENTE"
                                            ,"OK") = "OK", 
                                              if(count(distinct id_google_view)< (Select 
                                                                                  (count(distinct id_google_conta)-1)*0.51 as Google_ga 
                                                                                  From `desenvolvimento.clientes`)
                                                ," PUXAR RETROATIVOS"
                                                ,"OK")
                                        ,"ATENÇÂO ID_CLIENTE")
                                        ) as Status_ga_campanhas
                  from `desenvolvimento.ga_campanhas`
                    group by dt_sessao

              union all

              SELECT #GA4
               aData
                , "GA4"
                , IF(aContagem = dContagem, concat("qtd_id_ga4_sessoes: ", aContagem," | qtd_id_ga4_eventos: ", dContagem," | % de busca = ", 
                ((aContagem/(Select (count(distinct id_ga4)) as ga4 From `desenvolvimento.clientes`))*100) ," | Status: OK"), Concat("Nº de clientes diferentes, qtd_id_ga4_sessoes = ",aContagem," e qtd_id_ga4_eventos = ",dContagem," | % de busca = ",((aContagem/(Select (count(distinct id_ga4)) as ga4 From `desenvolvimento.clientes`))*100) )) AS Status_ga4_eventos
                  FROM 
                   (
                    SELECT 
                      if(a.data is null, d.data,a.data) as adata
                      , if(sum(a.Contagem) is null, 0, a.Contagem) as aContagem
                      , if(d.data is null, a.data, d.data) as ddata
                      , cast(if(d.Contagem is null, 0, sum(d.contagem)) as int64) as dContagem 
                      FROM (
                              SELECT DATE(dt_sessao) AS Data, COUNT(DISTINCT id_cliente) AS Contagem
                              FROM `desenvolvimento.ga4_sessoes`
                              GROUP BY Data
                              order by data desc
                            ) AS a
                            FULL JOIN (
                              SELECT DATE(dt_sessao) AS Data, COUNT(DISTINCT id_cliente) AS Contagem
                              FROM `desenvolvimento.ga4_eventos`
                              GROUP BY Data
                              order by data desc
                            ) AS d ON a.Data = d.Data
                            group by 1, 3, d.contagem, a.Contagem
                            order by adata desc)

              union all

              SELECT #GA4 Eventos
                aData
                , "GA4 Eventos"
                , IF(aContagem = dContagem, concat("qtd_id_ga4_eventos: ", aContagem," | qtd_id_ga4_sessoes: ", dContagem," | % de busca = ", 
                ((aContagem/(Select (count(distinct id_ga4)) as ga4 From `desenvolvimento.clientes`))*100) ," | Status: OK"), Concat("Nº de clientes diferentes, qtd_id_ga4_eventos = ",aContagem," e qtd_id_ga4_sessoes = ",dContagem," | % de busca = ",((aContagem/(Select (count(distinct id_ga4)) as ga4 From `desenvolvimento.clientes`))*100) )) AS Status_ga4_eventos
                  FROM 
                   (
                    SELECT 
                      if(a.data is null, d.data,a.data) as adata
                      , if(sum(a.Contagem) is null, 0, a.Contagem) as aContagem
                      , if(d.data is null, a.data, d.data) as ddata
                      , cast(if(d.Contagem is null, 0, sum(d.contagem)) as int64) as dContagem 
                      FROM (
                              SELECT DATE(dt_sessao) AS Data, COUNT(DISTINCT id_cliente) AS Contagem
                              FROM `desenvolvimento.ga4_eventos`
                              GROUP BY Data
                              order by data desc
                            ) AS a
                            FULL JOIN (
                              SELECT DATE(dt_sessao) AS Data, COUNT(DISTINCT id_cliente) AS Contagem
                              FROM `desenvolvimento.ga4_sessoes`
                              GROUP BY Data
                              order by data desc
                            ) AS d ON a.Data = d.Data
                            group by 1, 3, d.contagem, a.Contagem
                            order by adata desc)
                
              Union All

              select distinct  #TikTok
                data as Data
                , "TikTok"
              , concat("qtd id_cliente = ",count(distinct id_cliente), 
                        " | qtd account_id = ",count(distinct id_conta), " | % de busca = ", (count(distinct id_conta)/(Select 
                                            (count(distinct id_tiktok)) as tiktok_dados 
                                            From `desenvolvimento.clientes` where id_tiktok <> "0"))*100,
                        " | Status: ", if(
                                          if(count(distinct id_conta)-count(distinct id_cliente)>0
                                            ,"ATENÇÃO ID_CLIENTE"
                                            ,"OK") = "OK", 
                                              if(count(distinct id_conta)< (Select 
                                                                                  (count(distinct id_tiktok)-1)*0.51 as tiktok_ads 
                                                                                  From `desenvolvimento.clientes`)
                                                ," PUXAR RETROATIVOS"
                                                ,"OK")
                                        ,"ATENÇÂO ID_CLIENTE")
                                        ) as Status_tiktok_ads
                  from `desenvolvimento.tiktok_dados`
                    group by data

              Union all 

              select distinct  #Voxus ads
                data as Data
                , "Voxus"
              , concat("qtd id_cliente = ",count(distinct id)," | qtd token cadastrado = ", (Select 
                                            (count(distinct id_token_voxus)-1) as voxus_ads 
                                            From `desenvolvimento.clientes`) , " | % de busca = ", (count(distinct id)/(Select 
                                            (count(distinct id_token_voxus)) as voxus_dados 
                                            From `desenvolvimento.clientes` where id_token_voxus <> "0" ))*100 ,
                        " | Status: ", if(count(distinct id)< 
                                          (Select 
                                            (count(distinct id_token_voxus)-1)*0.51 as voxus_ads 
                                            From `desenvolvimento.clientes`)
                                                ," PUXAR RETROATIVOS"
                                                ,"OK")) as Status_voxus_ads
                  from `projeto.desenvolvimento.voxus_dados`
                    group by data
                
              Union All 

              select distinct  #Bing ads
                data as Data
                , "Bing"
              , concat("qtd id_cliente = ",count(distinct id)," | qtd conta cadastrada = ", (Select 
                                            (count(distinct id_bing)-1) as bing_ads 
                                            From `desenvolvimento.clientes`) , " | % de busca = ", (count(distinct id)/(Select 
                                            (count(distinct id_bing)-1) as bing_dados 
                                            From `desenvolvimento.clientes`))*100 ,
                        " | Status: ", if(count(distinct id)< 
                                          (Select 
                                            (count(distinct id_bing)-1)*0.51 as bing_ads 
                                            From `desenvolvimento.clientes`)
                                                ," PUXAR RETROATIVOS"
                                                ,"OK")) as Status_bing_ads
                  from `projeto.desenvolvimento.dados_bing_ads`
                    group by data

              Union All

              select distinct  #Blue ads
                data as Data
                , "Blue"
              , concat("qtd id_cliente = ",count(distinct id)," | qtd conta cadastrada = ", (Select 
                                            (count(distinct id_blue)-1) as blue_ads 
                                            From `desenvolvimento.clientes`) , " | % de busca = ", (count(distinct id)/(Select 
                                            (count(distinct id_blue)-1) as blue_dados 
                                            From `desenvolvimento.clientes`))*100 ,
                        " | Status: ", if(count(distinct id)< 
                                          (Select 
                                            (count(distinct id_blue)-1)*0.51 as blue_ads 
                                            From `desenvolvimento.clientes`)
                                                ," PUXAR RETROATIVOS"
                                                ,"OK")) as Status_blue_ads
                  from `projeto.desenvolvimento.dados_blue_ads`
                    group by data

              Union All 

              select distinct  #Criteo ads
                data as Data
                , "Criteo"
              , concat("qtd id_cliente = ",count(distinct id)," | qtd conta cadastrada = ", (Select 
                                            (count(distinct id_criteo)-1) as criteo_ads 
                                            From `desenvolvimento.clientes`) ,  " | % de busca = ", (count(distinct id)/(Select 
                                            (count(distinct id_criteo)-1) as criteo_dados 
                                            From `desenvolvimento.clientes`))*100 ,
                        " | Status: ", if(count(distinct id)< 
                                          (Select 
                                            (count(distinct id_criteo)-1)*0.51 as criteo_ads 
                                            From `desenvolvimento.clientes`)
                                                ," PUXAR RETROATIVOS"
                                                ,"OK")) as Status_criteo_ads
                  from `projeto.desenvolvimento.dados_criteo_ads`
                    group by data

              Union All

              select distinct  #Linkedin ads
                data as Data
                , "Linkedin"
              , concat("qtd id_cliente = ",count(distinct id)," | qtd conta cadastrada = ", (Select 
                                            (count(distinct id_linkedin)-1) as linkedin_ads 
                                            From `desenvolvimento.clientes`) , " | % de busca = ", (count(distinct id)/(Select 
                                            (count(distinct id_linkedin)-1) as linkedin_dados 
                                            From `desenvolvimento.clientes`))*100 ,
                        " | Status: ", if(count(distinct id)< 
                                          (Select 
                                            (count(distinct id_linkedin)-1)*0.51 as linkedin_ads 
                                            From `desenvolvimento.clientes`)
                                                ," PUXAR RETROATIVOS"
                                                ,"OK")) as Status_linkedin_ads
                  from `projeto.desenvolvimento.dados_linkedin_ads`
                    group by data

              Union All

              select distinct  #Pinterest ads
                data as Data
                , "Pinterest"
              , concat("qtd id_cliente = ",count(distinct id)," | qtd conta cadastrada = ", (Select 
                                            (count(distinct id_pinterest)-1) as pinterest_ads 
                                            From `desenvolvimento.clientes`) ,  " | % de busca = ", (count(distinct id)/(Select 
                                            (count(distinct id_pinterest)-1) as pinterest_dados 
                                            From `desenvolvimento.clientes`))*100 ,
                        " | Status: ", if(count(distinct id)< 
                                          (Select 
                                            (count(distinct id_pinterest)-1)*0.51 as pinterest_ads 
                                            From `desenvolvimento.clientes`)
                                                ," PUXAR RETROATIVOS"
                                                ,"OK")) as Status_pinterest_ads
                  from `projeto.desenvolvimento.dados_pinterest_ads`
                    group by data


              order by data desc, origem asc
                
          ) status on data.date = status.data and data.origem = status.origem order by date desc, origem asc

    ) as C 
  left join
    (
        Select #Buscar os possiveis IDs que deveriam de ter dados
  *
    FROM
    (
      Select #Google Ads
        data as date_start
        , "Google Ads" as origem
        , id_clientes_ausentes as retroativos
        FROM
          (
            WITH
              id_clientes_ativos AS (
                SELECT id_cliente 
                FROM `projeto.desenvolvimento.clientes`
                WHERE id_google_ads <> 0
              ),
              googleads_dados_por_dia AS (
                SELECT DISTINCT date, id_cliente
                FROM desenvolvimento.googleads_dados
              )
            SELECT
              d.data,
              concat("(",STRING_AGG
                (CAST(c.id_cliente AS STRING), ','),")") AS id_clientes_ausentes
            FROM
              id_clientes_ativos AS c
              CROSS JOIN (
                SELECT DISTINCT date as data
                FROM googleads_dados_por_dia
              ) AS d
              LEFT JOIN googleads_dados_por_dia AS b
                ON c.id_cliente = b.id_cliente
                AND d.data = b.date
            WHERE
              b.id_cliente IS NULL
            GROUP BY
              d.data
          )

      Union All

       Select #GA4 e GA4 Eventos
    dados.dt_sessao
    , media_class
    ,concat(dados.origem,": ",dados.id_cliente_faltante) as status

FROM

(
 Select #GA4
          dt_sessao
          ,"GA4" as origem
          , concat("(",string_agg(cast(id_cliente as string), ","),")") as id_cliente_faltante

            From
              (select distinct id_cliente, dt_sessao from `projeto.desenvolvimento.ga4_eventos` e where not exists (select 1 from `projeto.desenvolvimento.ga4_sessoes`s where e.id_cliente = s.id_cliente and e.dt_sessao = s.dt_sessao) order by 2 desc)
          group by 1

      Union All

        Select #GA4 Eventos
          dt_sessao
          ,"GA4 Eventos" as origem
          , concat("(",string_agg(cast(id_cliente as string), ","),")") as id_cliente_faltante

            From
              (select distinct id_cliente, dt_sessao from `projeto.desenvolvimento.ga4_sessoes` e where not exists (select 1 from `projeto.desenvolvimento.ga4_eventos`s where e.id_cliente = s.id_cliente and e.dt_sessao = s.dt_sessao) order by 2 desc)
          group by 1
) as Dados CROSS JOIN UNNEST(["GA4","GA4 Eventos"]) AS media_class
               
      Union All
      
      Select #GAU
        data as date_start
        , "GAU" as origem
        , id_clientes_ausentes as retroativos
        FROM
          (
            WITH
              id_clientes_ativos AS (
                SELECT id_cliente 
                FROM `projeto.desenvolvimento.clientes`
                WHERE id_google_conta <> 0
              ),
              ga_campanhas_por_dia AS (
                SELECT DISTINCT dt_sessao, id_cliente
                FROM desenvolvimento.ga_campanhas
              )
            SELECT
              d.data,
              concat("(",STRING_AGG
                (CAST(c.id_cliente AS STRING), ','),")") AS id_clientes_ausentes
            FROM
              id_clientes_ativos AS c
              CROSS JOIN (
                SELECT DISTINCT dt_sessao as data
                FROM ga_campanhas_por_dia
              ) AS d
              LEFT JOIN ga_campanhas_por_dia AS b
                ON c.id_cliente = b.id_cliente
                AND d.data = b.dt_sessao
            WHERE
              b.id_cliente IS NULL
            GROUP BY
              d.data
          )

      Union All

      Select #Voxus
        data as date_start
        , "Voxus" as origem
        , id_clientes_ausentes as retroativos
        FROM
          (
            WITH
              id_clientes_ativos AS (
                SELECT id_cliente 
                FROM `projeto.desenvolvimento.clientes`
                WHERE id_token_voxus <> "0"
              ),
              voxus_dados_por_dia AS (
                SELECT DISTINCT data, id
                FROM desenvolvimento.voxus_dados
              )
            SELECT
              d.data,
              concat("(",STRING_AGG
                (CAST(c.id_cliente AS STRING), ','),")") AS id_clientes_ausentes
            FROM
              id_clientes_ativos AS c
              CROSS JOIN (
                SELECT DISTINCT data
                FROM voxus_dados_por_dia
              ) AS d
              LEFT JOIN voxus_dados_por_dia AS b
                ON c.id_cliente = b.id
                AND d.data = b.data
            WHERE
              b.id IS NULL
            GROUP BY
              d.data
          )

      Union All

      Select #TikTok
        data as date_start
        , "TikTok" as origem
        , id_clientes_ausentes as retroativos
        FROM
          (
            WITH
              id_clientes_ativos AS (
                SELECT id_cliente 
                FROM `projeto.desenvolvimento.clientes`
                WHERE id_tiktok <> "0"
              ),
              tiktok_dados_por_dia AS (
                SELECT DISTINCT data, id_cliente
                FROM desenvolvimento.tiktok_dados
              )
            SELECT
              d.data,
              concat("(",STRING_AGG
                (CAST(c.id_cliente AS STRING), ','),")") AS id_clientes_ausentes
            FROM
              id_clientes_ativos AS c
              CROSS JOIN (
                SELECT DISTINCT data
                FROM tiktok_dados_por_dia
              ) AS d
              LEFT JOIN tiktok_dados_por_dia AS b
                ON c.id_cliente = b.id_cliente
                AND d.data = b.data
            WHERE
              b.id_cliente IS NULL
            GROUP BY
              d.data
          )

      Union All

      Select #Pinterest
        data as date_start
        , "Pinterest" as origem
        , id_clientes_ausentes as retroativos
        FROM
          (
            WITH
              id_clientes_ativos AS (
                SELECT id_cliente 
                FROM `projeto.desenvolvimento.clientes`
                WHERE id_pinterest <> "0"
              ),
              dados_pinterest_ads_por_dia AS (
                SELECT DISTINCT data, id
                FROM desenvolvimento.dados_pinterest_ads
              )
            SELECT
              d.data,
              concat("(",STRING_AGG
                (CAST(c.id_cliente AS STRING), ','),")") AS id_clientes_ausentes
            FROM
              id_clientes_ativos AS c
              CROSS JOIN (
                SELECT DISTINCT data
                FROM dados_pinterest_ads_por_dia
              ) AS d
              LEFT JOIN dados_pinterest_ads_por_dia AS b
                ON c.id_cliente = b.id
                AND d.data = b.data
            WHERE
              b.id IS NULL
            GROUP BY
              d.data
          )

      Union All

      Select #LinkedIn
        data as date_start
        , "Linkedin" as origem
        , id_clientes_ausentes as retroativos
        FROM
          (
            WITH
              id_clientes_ativos AS (
                SELECT id_cliente 
                FROM `projeto.desenvolvimento.clientes`
                WHERE id_linkedin <> "0"
              ),
              dados_linkedin_ads_por_dia AS (
                SELECT DISTINCT data, id
                FROM desenvolvimento.dados_linkedin_ads
              )
            SELECT
              d.data,
              concat("(",STRING_AGG
                (CAST(c.id_cliente AS STRING), ','),")") AS id_clientes_ausentes
            FROM
              id_clientes_ativos AS c
              CROSS JOIN (
                SELECT DISTINCT data
                FROM dados_linkedin_ads_por_dia
              ) AS d
              LEFT JOIN dados_linkedin_ads_por_dia AS b
                ON c.id_cliente = b.id
                AND d.data = b.data
            WHERE
              b.id IS NULL
            GROUP BY
              d.data
          )

      Union All

      Select #Criteo
        data as date_start
        , "Criteo" as origem
        , id_clientes_ausentes as retroativos
        FROM
          (
            WITH
              id_clientes_ativos AS (
                SELECT id_cliente 
                FROM `projeto.desenvolvimento.clientes`
                WHERE id_criteo <> 0
              ),
              dados_criteo_ads_por_dia AS (
                SELECT DISTINCT data, id
                FROM desenvolvimento.dados_criteo_ads
              )
            SELECT
              d.data,
              concat("(",STRING_AGG
                (CAST(c.id_cliente AS STRING), ','),")") AS id_clientes_ausentes
            FROM
              id_clientes_ativos AS c
              CROSS JOIN (
                SELECT DISTINCT data
                FROM dados_criteo_ads_por_dia
              ) AS d
              LEFT JOIN dados_criteo_ads_por_dia AS b
                ON c.id_cliente = b.id
                AND d.data = b.data
            WHERE
              b.id IS NULL
            GROUP BY
              d.data
          )

      Union All

      Select #Blue
        data as date_start
        , "Blue" as origem
        , id_clientes_ausentes as retroativos
        FROM
          (
            WITH
              id_clientes_ativos AS (
                SELECT id_cliente 
                FROM `projeto.desenvolvimento.clientes`
                WHERE id_blue <> "0"
              ),
              dados_blue_ads_por_dia AS (
                SELECT DISTINCT data, id
                FROM desenvolvimento.dados_blue_ads
              )
            SELECT
              d.data,
              concat("(",STRING_AGG
                (CAST(c.id_cliente AS STRING), ','),")") AS id_clientes_ausentes
            FROM
              id_clientes_ativos AS c
              CROSS JOIN (
                SELECT DISTINCT data
                FROM dados_blue_ads_por_dia
              ) AS d
              LEFT JOIN dados_blue_ads_por_dia AS b
                ON c.id_cliente = b.id
                AND d.data = b.data
            WHERE
              b.id IS NULL
            GROUP BY
              d.data
          )

      Union All

      Select #Bing
        data as date_start
        , "Bing" as origem
        , id_clientes_ausentes as retroativos
        FROM
          (
            WITH
              id_clientes_ativos AS (
                SELECT id_cliente 
                FROM `projeto.desenvolvimento.clientes`
                WHERE id_bing <> 0
              ),
              dados_bing_ads_por_dia AS (
                SELECT DISTINCT data, id
                FROM desenvolvimento.dados_bing_ads
              )
            SELECT
              d.data,
              concat("(",STRING_AGG
                (CAST(c.id_cliente AS STRING), ','),")") AS id_clientes_ausentes
            FROM
              id_clientes_ativos AS c
              CROSS JOIN (
                SELECT DISTINCT data
                FROM dados_bing_ads_por_dia
              ) AS d
              LEFT JOIN dados_bing_ads_por_dia AS b
                ON c.id_cliente = b.id
                AND d.data = b.data
            WHERE
              b.id IS NULL
            GROUP BY
              d.data
          )

      Union All

      Select #Facebook Actions
  date_start
  ,"Facebook Actions"
  , id_cliente_faltante
  from
    (          
      WITH
        dados AS (
        SELECT
          DISTINCT date_start,
          id_cliente
        FROM
          desenvolvimento.fb_dados),
        actions AS (
        SELECT
          DISTINCT date_start,
          id_cliente
        FROM
          desenvolvimento.fb_actions)
      SELECT
        dados.date_start,
        concat("(",STRING_AGG(CAST(dados.id_cliente AS STRING), ','),")") AS id_cliente_faltante
      FROM
        dados
        LEFT JOIN (
          SELECT
            date_start,
            id_cliente
          FROM
            actions
        ) AS actions
        ON
          dados.date_start = cast(actions.date_start as date)
          AND dados.id_cliente = actions.id_cliente
      WHERE
        actions.id_cliente IS NULL
      GROUP BY
        dados.date_start
      ORDER BY
        dados.date_start
    )

    Union All

    Select #Facebook
        date_start as date_start
        , "Facebook dados" as origem
        , id_clientes_ausentes as retroativos
        FROM
          (
            WITH
              id_clientes_ativos AS (
                SELECT id_cliente 
                FROM `projeto.desenvolvimento.clientes`
                WHERE id_facebook <> "0"
              ),
              fb_dados_por_dia AS (
                SELECT DISTINCT date_start, id_cliente
                FROM desenvolvimento.fb_dados
              )
            SELECT
              d.date_start,
              concat("(",STRING_AGG
                (CAST(c.id_cliente AS STRING), ','),")") AS id_clientes_ausentes
            FROM
              id_clientes_ativos AS c
              CROSS JOIN (
                SELECT DISTINCT date_start
                FROM fb_dados_por_dia
              ) AS d
              LEFT JOIN fb_dados_por_dia AS b
                ON c.id_cliente = b.id_cliente
                AND d.date_start = b.date_start
            WHERE
              b.id_cliente IS NULL
            GROUP BY
              d.date_start
          )

    )
    ) as B on C.date = B.date_start and C.origem = B.origem 
   Where C.date < CURRENT_DATE() order by date desc, origem asc