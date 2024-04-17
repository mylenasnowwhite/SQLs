#Apagar todos os dados da tabela
delete from  `adtail-bq.desenvolvimento.BD_UNION_IM` where datas >= date_add(date(format_datetime('%Y-%m-01',current_date())),interval -1 month);
#Limpeza finalizada
#Inicio da inserção de dados
INSERT INTO `adtail-bq.desenvolvimento.BD_UNION_IM` (Datas,Conta_Id, Origem,Origem_Midia, Anuncios, Campanhas, ImgURL,Nome_Eventos, Investimento, Impressoes, Cliques, Link_Cliques, Conversoes, Engajamento, Alcance, Frequencia, Sessoes,Sessoes_Engajadas ,Rejeicoes, Transacoes, Receita, Duracao_media_da_sessao, Visitas_Paginas, Qtd_Itens, Novos_Usuarios, Compras_Unicas, Meta_01, Meta_02, Meta_03, Meta_04, Meta_05,Contagem_Eventos,Transacoes_Eventos,Receita_Eventos,Conversoes_Eventos,Leads_GA,Leads_Totais_GA_Conversoes, Source, Channel)

  SELECT #Facebook
    date_start as Datas
    , IFNULL(id_cliente,0) as Conta_ID
    ,'Facebook' as Origem
    , NULL as Origem_Midia
    , ad_name as Anuncios
    , campaign_name	as Campanhas
    , cast(Null as string) as ImgURL
    , cast(Null as string) as Nome_Eventos
    , coalesce(sum(spend), 0) as Investimento
    , coalesce(sum(impressions),0) as Impressoes
    , coalesce(sum(clicks),0) as Cliques
    , 0 as Link_Cliques
    , 0 as Conversoes
    , 0 as Engajamento
    , coalesce(sum(cast(reach as 	int64)),0) as Alcance
    , coalesce(sum(cast (frequency as 	numeric)),0) as Frequencia
    , 0 as Sessoes
    , 0 as Sessoes_Engajadas
    , 0 as Rejeicoes
    , 0 as Transacoes
    , 0 as Receita
    , 0 as Duracao_media_da_sessao
    , 0 as Visitas_Paginas
    , 0 as Itens
    , 0 as Novos_Usuarios
    , 0 as Compras_Unicas
    , 0 as Meta_01
    , 0 as Meta_02
    , 0 as Meta_03
    , 0 as Meta_04
    , 0 as Meta_05
    , 0 as Contagem_Eventos
    , 0 as Transacoes_Eventos
    , 0 as Receita_Eventos
    , 0 as Conversoes_Eventos
    , 0 as Leads_GA
    , 0 as Leads_Totais_GA_Conversoes
    , cast(Null as string) as Source
    , cast(Null as string) as Channel
    FROM 
          (Select distinct * from `adtail-bq.desenvolvimento.fb_dados` where id_cliente > 0 and date_start >= date_add(date(format_datetime('%Y-%m-01',current_date())),interval -1 month))
        group by date_start, id_cliente, ad_name, campaign_name

  UNION ALL

  SELECT #Criteo
    Data as Datas
    , IFNULL(CAST(ID as int),0) as Conta_ID
    ,'Criteo' as origem
    , Null as Origem_Midia
    , midia_adset_name as Anuncios
    , NULL	as Campanhas
    , cast(Null as string) as ImgURL
    , cast(Null as string) as Nome_Eventos
    , coalesce(sum(midia_custo_vl),0) as Investimento
    , coalesce(sum(midia_displays_vl),0) as Impressoes
    , coalesce(sum(midia_cliques_vl),0) as Cliques
    , 0 as Link_Cliques
    , 0 as Conversoes
    , 0 as Engajamento
    , 0 as Alcance
    , 0 as Frequencia
    , 0 as Sessoes
    , 0 as Sessoes_Engajadas
    , 0 as Rejeicoes
    , 0 as Transacoes
    , 0 as Receita
    , 0 as Duracao_media_da_sessao
    , 0 as Visitas_Paginas
    , 0 as Itens
    , 0 as Novos_Usuarios
    , 0 as Compras_Unicas
    , 0 as Meta_01
    , 0 as Meta_02
    , 0 as Meta_03
    , 0 as Meta_04
    , 0 as Meta_05
    , 0 as Contagem_Eventos
    , 0 as Transacoes_Eventos
    , 0 as Receita_Eventos
    , 0 as Conversoes_Eventos
    , 0 as Leads_GA
    , 0 as Leads_Totais_GA_Conversoes
    , cast(Null as string) as Source
    , cast(Null as string) as Channel
    FROM 
        (Select distinct * from`adtail-bq.desenvolvimento.dados_criteo_ads` where data >= date_add(date(format_datetime('%Y-%m-01',current_date())),interval -1 month))
      group by Data, ID, midia_adset_name

  UNION ALL

  SELECT #GA
    dt_sessao as Datas
    , IFNULL(CAST(id_cliente as int),0) as Conta_ID
    , "Google Analytics" as origem
    , ds_origem_midia as Origem_Midia
    , Cast(Null as string) as Anuncios
    , replace(ds_campanha, "+"," ")	as Campanhas
    , cast(Null as string) as ImgURL
    , cast(Null as string) as Nome_Eventos
    , coalesce(sum(vl_custo), 0) as Investimento
    , coalesce(sum(qtd_impressoes), 0) as Impressoes
    , coalesce(sum(qtd_clicks),0) as Cliques
    , 0 as Link_Cliques
    , 0 as Conversoes
    , 0 as Engajamento
    , 0 as Alcance
    , 0 as Frequencia
    , coalesce(sum(qtd_sessoes),0) as Sessoes
    , 0 as Sessoes_Engajadas
    , coalesce(sum(qtd_bounces),0) as Rejeicoes
    , coalesce(sum(qtd_transacoes),0) as Transacoes
    , coalesce(sum(vl_receita),0) as Receita
    , coalesce(sum(qtd_duracao_sessao),0) as Duracao_da_sessao
    , coalesce(sum(qtd_visitas_paginas),0) as Visitas_Paginas
    , coalesce(sum(qtd_itens),0) as Itens
    , coalesce(sum(qtd_novos_usuarios),0) as Novos_Usuarios
    , coalesce(sum(qtd_compras_unicas),0) as Compras_Unicas
    , coalesce(sum(vl_meta_01),0) as Meta_01
    , coalesce(sum(vl_meta_02),0) as Meta_02
    , coalesce(sum(vl_meta_03),0) as Meta_03
    , coalesce(sum(vl_meta_04),0) as Meta_04
    , coalesce(sum(vl_meta_05),0) as Meta_05
    , 0 as Contagem_Eventos
    , 0 as Transacoes_Eventos
    , 0 as Receita_Eventos
    , 0 as Conversoes_Eventos
    , 0 as Leads_GA
    , 0 as Leads_Totais_GA_Conversoes
    , cast(Null as string) as Source
    , cast(Null as string) as Channel
      FROM 
          (Select distinct * from (Select distinct * from`adtail-bq.desenvolvimento.ga_campanhas` where dt_sessao >= date_add(date(format_datetime('%Y-%m-01',current_date())),interval -1 month)) e  where exists (select id_cliente from `adtail-bq.desenvolvimento.clientes` s where e.id_cliente = s.id_cliente and s.busca_gads = 0))
        group by dt_sessao, id_cliente, ds_origem_midia, ds_campanha

  UNION ALL

  SELECT  #Bing
    Data as Datas
    , IFNULL(CAST(ID as int),0) as Conta_ID
    ,'Bing' as origem
    , cast(Null as string) as Origem_Midia
    , cast(Null as string) as Anuncios
    , midia_campanha_name	as Campanhas
    , cast(Null as string) as ImgURL
    , cast(Null as string) as Nome_Eventos
    , coalesce(sum(midia_custo_vl),0) as Investimento
    , coalesce(sum(midia_impressoes_vl),0) as Impressoes
    , coalesce(sum(midia_cliques_vl),0) as Cliques
    , 0 as Link_Cliques
    , 0 as Conversoes
    , 0 as Engajamento
    , 0 as Alcance
    , 0 as Frequencia
    , 0 as Sessoes
    , 0 as Sessoes_Engajadas
    , 0 as Rejeicoes
    , 0 as Transacoes
    , 0 as Receita
    , 0 as Duracao_media_da_sessao
    , 0 as Visitas_Paginas
    , 0 as Itens
    , 0 as Novos_Usuarios
    , 0 as Compras_Unicas
    , 0 as Meta_01
    , 0 as Meta_02
    , 0 as Meta_03
    , 0 as Meta_04
    , 0 as Meta_05
    , 0 as Contagem_Eventos
    , 0 as Transacoes_Eventos
    , 0 as Receita_Eventos
    , 0 as Conversoes_Eventos
    , 0 as Leads_GA
    , 0 as Leads_Totais_GA_Conversoes
    , cast(Null as string) as Source
    , cast(Null as string) as Channel
    FROM 
        (Select Distinct * from `adtail-bq.desenvolvimento.dados_bing_ads` where data >= date_add(date(format_datetime('%Y-%m-01',current_date())),interval -1 month))
      group by Data, ID, midia_campanha_name

  UNION ALL

  SELECT #Pinterest
    Data as Datas
    ,IFNULL(CAST(ID as int),0) as Conta_ID
    ,'Pinterest' as origem
    , cast(Null as string) as Origem_Midia
    , cast(Null as string) as Anuncios
    , midia_campaign_name	as Campanhas
    , cast(Null as string) as ImgUR
    , cast(Null as string) as Nome_Eventos
    , coalesce(sum(midia_custo_vl),0) as Investimento
    , coalesce(sum(midia_impressoes_vl),0) as Impressoes
    , coalesce(sum(midia_cliques_vl),0) as Cliques
    , 0 as Link_Cliques
    , 0 as Conversoes
    , 0 as Engajamento
    , 0 as Alcance
    , 0 as Frequencia
    , 0 as Sessoes
    , 0 as Sessoes_Engajadas
    , 0 as Rejeicoes
    , 0 as Transacoes
    , 0 as Receita
    , 0 as Duracao_media_da_sessao
    , 0 as Visitas_Paginas
    , 0 as Itens
    , 0 as Novos_Usuarios
    , 0 as Compras_Unicas
    , 0 as Meta_01
    , 0 as Meta_02
    , 0 as Meta_03
    , 0 as Meta_04
    , 0 as Meta_05
    , 0 as Contagem_Eventos
    , 0 as Transacoes_Eventos
    , 0 as Receita_Eventos
    , 0 as Conversoes_Eventos
    , 0 as Leads_GA
    , 0 as Leads_Totais_GA_Conversoes
    , cast(Null as string) as Source
    , cast(Null as string) as Channel
    FROM  (Select Distinct * from `adtail-bq.desenvolvimento.dados_pinterest_ads` where data >= date_add(date(format_datetime('%Y-%m-01',current_date())),interval -1 month))
      group by Data, ID, midia_campaign_name

  UNION ALL

  SELECT #Blue
    Data as Datas
    , IFNULL(CAST(ID as int),0) as Conta_ID
    ,'Blue' as origem
    , cast(Null as string) as Origem_Midia
    , cast(Null as string) as Anuncios
    , midia_campaign_name	as Campanhas
    , cast(Null as string) as ImgURL
    , cast(Null as string) as Nome_Eventos
    , coalesce(sum(midia_custo_vl),0) as Investimento
    , coalesce(sum(midia_impressoes_vl),0) as Impressoes
    , coalesce(sum(midia_cliques_vl),0) as Cliques
    , 0 as Link_Cliques
    , coalesce(sum(cast(midia_conversoes_vl as int64)),0) as Conversoes
    , 0 as Engajamento
    , 0 as Alcance
    , 0 as Frequencia
    , 0 as Sessoes
    , 0 as Sessoes_Engajadas
    , 0 as Rejeicoes
    , 0 as Transacoes
    , 0 as Receita
    , 0 as Duracao_media_da_sessao
    , 0 as Visitas_Paginas
    , 0 as Itens
    , 0 as Novos_Usuarios
    , 0 as Compras_Unicas
    , 0 as Meta_01
    , 0 as Meta_02
    , 0 as Meta_03
    , 0 as Meta_04
    , 0 as Meta_05
    , 0 as Contagem_Eventos
    , 0 as Transacoes_Eventos
    , 0 as Receita_Eventos
    , 0 as Conversoes_Eventos
    , 0 as Leads_GA
    , 0 as Leads_Totais_GA_Conversoes
    , cast(Null as string) as Source
    , cast(Null as string) as Channel
    FROM 
        (select 
            max(id) as id
            , data
            , max(midia_custo_vl) as midia_custo_vl
            , max(midia_cliques_vl) as midia_cliques_vl
            , max(midia_conversoes_vl) as midia_conversoes_vl
            , midia_campaign_name
            , max(midia_impressoes_vl) as midia_impressoes_vl
            From (select distinct * from desenvolvimento.dados_blue_ads)
            where data >= date_add(date(format_datetime('%Y-%m-01',current_date())),interval -1 month)
            group by 6, data
            order by data desc)
      group by Data, ID, midia_campaign_name
  UNION ALL

  SELECT #FACEBOOk ACTIONS
      cast(date_start as Date) as Datas
    , IFNULL(CAST(id_cliente as int),0) as Conta_ID
    ,'Facebook' as origem
    , cast(Null as string) as Origem_Midia
    , cast(Null as string) as Anuncios
    , campaign_name	as Campanhas
    , cast(Null as string) as ImgURL
    , cast(Null as string) as Nome_Eventos
    , 0 as Investimento
    , 0 as Impressoes
    , 0 as Cliques
    , coalesce(Sum(link_click),0) as Link_Cliques
    , coalesce(Sum(lead),0) as Conversoes
    , coalesce(Sum(post_engagement),0) as Engajamento
    , 0 as Alcance
    , 0 as Frequencia
    , 0 as Sessoes
    , 0 as Sessoes_Engajadas
    , 0 as Rejeicoes
    , 0 as Transacoes
    , 0 as Receita
    , 0 as Duracao_media_da_sessao
    , 0 as Visitas_Paginas
    , 0 as Itens
    , 0 as Novos_Usuarios
    , 0 as Compras_Unicas
    , 0 as Meta_01
    , 0 as Meta_02
    , 0 as Meta_03
    , 0 as Meta_04
    , 0 as Meta_05
    , 0 as Contagem_Eventos
    , 0 as Transacoes_Eventos
    , 0 as Receita_Eventos
    , 0 as Conversoes_Eventos
    , 0 as Leads_GA
    , 0 as Leads_Totais_GA_Conversoes
    , cast(Null as string) as Source
    , cast(Null as string) as Channel
    FROM 
        (
          (
      SELECT distinct
        a.id_cliente
        ,a.campaign_id
        ,b.campaign_name
        ,a.account_id
        ,cast(a.date_start as Date) as date_start
        ,a.link_click
        ,a.lead
        ,a.page_engagement
        ,a.post_engagement
        ,a.post_reaction
        ,a.landing_page_view
        
      FROM 
        (
          SELECT distinct
              id_cliente
              ,campaign_id
              ,account_id
              ,date_start
              ,date_stop 
              ,IFNULL(a.link_click,0) link_click
              ,IFNULL(a.post_engagement,0) post_engagement
              ,IFNULL(a.page_engagement,0) page_engagement
              ,IFNULL(a.lead,0) lead
              ,IFNULL(a.post_reaction,0) post_reaction
              ,IFNULL(a.landing_page_view,0) landing_page_view
              
              FROM (
                SELECT DISTINCT * FROM
                (SELECT DISTINCT
                    id_cliente
                    ,campaign_id
                    ,account_id
                    ,date_start
                    ,date_stop
                    ,a.action_type
                    ,a.value 
                  FROM  `adtail-bq.desenvolvimento.fb_actions`, UNNEST(actions) as a) 
                  PIVOT(SUM(value) FOR action_type IN ('link_click','post_engagement','page_engagement', 'lead', 'post_reaction', 'landing_page_view' ))
                  where cast(date_start as date) >= date_add(date(format_datetime('%Y-%m-01',current_date())),interval -1 month)
                  ) a

                ) a
                left join  `adtail-bq.desenvolvimento.fb_dados` b on b.account_id = a.account_id and cast(b.campaign_id as int) = a.campaign_id
    where cast(b.date_start as date) >= date_add(date(format_datetime('%Y-%m-01',current_date())),interval -1 month)
      )
            )

          group by date_start, id_cliente, campaign_name

  UNION ALL

  SELECT #LinkedIn
    Data as Datas
    ,IFNULL(CAST(ID as int),0) as Conta_ID
    ,'LinkedIn' as origem
    , cast(Null as string) as Origem_Midia
    , cast(Null as string) as Anuncios
    , midia_campaign_name	as Campanhas
    , cast(Null as string) as ImgURL
    , cast(Null as string) as Nome_Eventos
    , coalesce(sum(midia_custo_vl),0) as Investimento
    , coalesce(sum(midia_impressoes_vl),0) as Impressoes
    , coalesce(sum(midia_cliques_vl),0) as Cliques
    , 0 as Link_Cliques
    , coalesce(sum(midia_conversao_vl),0) as Conversoes
    , 0 as Engajamento
    , 0 as Alcance
    , 0 as Frequencia
    , 0 as Sessoes
    , 0 as Sessoes_Engajadas
    , 0 as Rejeicoes
    , 0 as Transacoes
    , 0 as Receita
    , 0 as Duracao_media_da_sessao
    , 0 as Visitas_Paginas
    , 0 as Itens
    , 0 as Novos_Usuarios
    , 0 as Compras_Unicas
    , 0 as Meta_01
    , 0 as Meta_02
    , 0 as Meta_03
    , 0 as Meta_04
    , 0 as Meta_05
    , 0 as Contagem_Eventos
    , 0 as Transacoes_Eventos
    , 0 as Receita_Eventos
    , 0 as Conversoes_Eventos
    , 0 as Leads_GA
    , 0 as Leads_Totais_GA_Conversoes
    , cast(Null as string) as Source
    , cast(Null as string) as Channel
    FROM 
        (Select distinct * from`adtail-bq.desenvolvimento.dados_linkedin_ads`where data >= date_add(date(format_datetime('%Y-%m-01',current_date())),interval -1 month))
      group by Data, ID,midia_campaign_name

  UNION ALL

  SELECT #Voxus
    Data as Datas
    , ID as Conta_ID
    ,'Voxus' as origem
    , cast(Null as string) as Origem_Midia
    , cast(Null as string) as Anuncios
    , null as Campanhas
    , Null as ImgURL
    , cast(Null as string) as Nome_Eventos
    , coalesce(sum(value),0) as Investimento
    , coalesce(sum(impressions),0) as Impressoes
    , 0 as Cliques
    , 0 as Link_Cliques
    , coalesce(sum(cast(conversions as int64)),0) as Conversoes
    , 0 as Engajamento
    , 0 as Alcance
    , 0 as Frequencia
    , 0 as Sessoes
    , 0 as Sessoes_Engajadas
    , 0 as Rejeicoes
    , 0 as Transacoes
    , 0 as Receita
    , 0 as Duracao_media_da_sessao
    , 0 as Visitas_Paginas
    , 0 as Itens
    , 0 as Novos_Usuarios
    , 0 as Compras_Unicas
    , 0 as Meta_01
    , 0 as Meta_02
    , 0 as Meta_03
    , 0 as Meta_04
    , 0 as Meta_05
    , 0 as Contagem_Eventos
    , 0 as Transacoes_Eventos
    , 0 as Receita_Eventos
    , 0 as Conversoes_Eventos
    , 0 as Leads_GA
    , 0 as Leads_Totais_GA_Conversoes
    , cast(Null as string) as Source
    , cast(Null as string) as Channel
    FROM 
        (Select distinct * from`adtail-bq.desenvolvimento.voxus_dados` where data >= date_add(date(format_datetime('%Y-%m-01',current_date())),interval -1 month))
      group by Data, ID

  UNION ALL 

  SELECT #TikTok
    data as Datas
    , id_cliente as Conta_ID
    ,'TikTok' as origem
    , cast(Null as string) as Origem_Midia
    , cast(Null as string) as Anuncios
    , campanha_nome as Campanhas
    , Null as ImgURL
    , cast(Null as string) as Nome_Eventos
    , coalesce(sum(custo_vl),0) as Investimento
    , coalesce(sum(impressoes_vl),0) as Impressoes
    , coalesce(sum(cliques_vl),0) as Cliques
    , 0 as Link_Cliques
    , 0 as Conversoes
    , 0 as Engajamento
    , coalesce(sum(alcance_vl),0) as Alcance
    , coalesce(sum(frequencia_vl),0) as Frequencia
    , 0 as Sessoes
    , 0 as Sessoes_Engajadas
    , 0 as Rejeicoes
    , 0 as Transacoes
    , 0 as Receita
    , 0 as Duracao_media_da_sessao
    , coalesce(sum(visitas_vl),0) as Visitas_Paginas
    , 0 as Itens
    , 0 as Novos_Usuarios
    , 0 as Compras_Unicas
    , 0 as Meta_01
    , 0 as Meta_02
    , 0 as Meta_03
    , 0 as Meta_04
    , 0 as Meta_05
    , 0 as Contagem_Eventos
    , 0 as Transacoes_Eventos
    , 0 as Receita_Eventos
    , 0 as Conversoes_Eventos
    , 0 as Leads_GA
    , 0 as Leads_Totais_GA_Conversoes
    , cast(Null as string) as Source
    , cast(Null as string) as Channel
    FROM 
        (Select distinct * from`adtail-bq.desenvolvimento.tiktok_dados` where data >= date_add(date(format_datetime('%Y-%m-01',current_date())),interval -1 month))
    group by Data, id_cliente, campanha_nome

  Union All

  SELECT #Google Ads
    date as Datas
    , id_cliente as Conta_ID
    ,'Google Ads' as origem
    , cast(Null as string) as Origem_Midia
    , ad_name as Anuncios
    , campaign_name as Campanhas
    , Null as ImgURL
    , cast(Null as string) as Nome_Eventos
    , coalesce(sum(cost),0) as Investimento
    , coalesce(sum(impressions),0) as Impressoes
    , coalesce(sum(clicks),0) as Cliques
    , 0 as Link_Cliques
    , coalesce(sum(cast(conversions as int64))) as Conversoes
    , 0 as Engajamento
    , 0 as Alcance
    , 0 as Frequencia
    , 0 as Sessoes
    , 0 as Sessoes_Engajadas
    , 0 as Rejeicoes
    , 0 as Transacoes
    , 0 as Receita
    , 0 as Duracao_media_da_sessao
    , 0 as Visitas_Paginas
    , 0 as Itens
    , 0 as Novos_Usuarios
    , 0 as Compras_Unicas
    , 0 as Meta_01
    , 0 as Meta_02
    , 0 as Meta_03
    , 0 as Meta_04
    , 0 as Meta_05
    , 0 as Contagem_Eventos
    , 0 as Transacoes_Eventos
    , 0 as Receita_Eventos
    , 0 as Conversoes_Eventos
    , 0 as Leads_GA
    , 0 as Leads_Totais_GA_Conversoes
    , cast(Null as string) as Source
    , cast(Null as string) as Channel
    FROM 
        (Select * from (Select distinct * from`adtail-bq.desenvolvimento.googleads_dados` where date >= date_add(date(format_datetime('%Y-%m-01',current_date())),interval -1 month)) 
        e    where exists (select id_cliente from `adtail-bq.desenvolvimento.clientes` s where e.id_cliente = s.id_cliente and s.busca_gads = 1))
    group by Date, id_cliente, campaign_name, ad_name

  Union All

  SELECT #Google Analytics 4 tabela Sessões
    dt_sessao as Datas
    , id_cliente as Conta_ID
    ,'Google Analytics 4' as origem
    , ds_origem_midia_sessao as Origem_Midia
    , cast(Null as string) as Anuncios
    , ds_campanha_sessao as Campanhas
    , Null as ImgURL
    , cast(Null as string) as Nome_Eventos
    , 0 as Investimento
    , 0 as Impressoes
    , 0 as Cliques
    , 0 as Link_Cliques
    , 0 as Conversoes
    , 0 as Engajamento
    , 0 as Alcance
    , 0 as Frequencia
    , coalesce(sum(qtd_sessoes),0) as Sessoes
    , coalesce(sum(qtd_sessoes_engajadas),0) as Sessoes_Engajadas
    , coalesce(sum(qtd_sessoes)-sum(qtd_sessoes_engajadas),0) as Rejeicoes
    , coalesce(sum(qtd_transacoes),0) as Transacoes
    , coalesce(sum(vl_receita),0) as Receita
    , 0 as Duracao_media_da_sessao
    , 0 as Visitas_Paginas
    , 0 as Itens
    , 0 as Novos_Usuarios
    , 0 as Compras_Unicas
    , 0 as Meta_01
    , 0 as Meta_02
    , 0 as Meta_03
    , 0 as Meta_04
    , 0 as Meta_05
    , coalesce(sum(qtd_eventos)) as Contagem_Eventos
    , 0 as Transacoes_Eventos
    , 0 as Receita_Eventos
    , 0 as Conversoes_Eventos
    , coalesce(sum(cast(qtd_conversoes as int64))) as Leads_GA
    , 0 as Leads_Totais_GA_Conversoes
    , cast(Null as string) as Source
    , cast(Null as string) as Channel
    FROM 
        (Select * from (Select distinct * from`adtail-bq.desenvolvimento.ga4_sessoes` where dt_sessao >= date_add(date(format_datetime('%Y-%m-01',current_date())),interval -1 month)) 
        e    where exists (select id_cliente from `adtail-bq.desenvolvimento.clientes` s where e.id_cliente = s.id_cliente and s.busca_gads = 1))
    group by dt_sessao, id_cliente, ds_campanha_sessao, ds_origem_midia_sessao
  Union All

  SELECT #Google Analytics 4 tabela Eventos
    dt_sessao as Datas
    , e.id_cliente as Conta_ID
    ,'Google Analytics 4 Eventos' as origem
    , ds_origem_midia_sessao as Origem_Midia
    , cast(Null as string) as Anuncios
    , ds_campanha_sessao as Campanhas
    , Null as ImgURL
    , ds_evento as Nome_Eventos
    , 0 as Investimento
    , 0 as Impressoes
    , 0 as Cliques
    , 0 as Link_Cliques
    , 0 as Conversoes
    , 0 as Engajamento
    , 0 as Alcance
    , 0 as Frequencia
    , 0 as Sessoes
    , 0 as Sessoes_Engajadas
    , 0 as Rejeicoes
    , 0 as Transacoes
    , 0 as Receita
    , 0 as Duracao_media_da_sessao
    , 0 as Visitas_Paginas
    , 0 as Itens
    , 0 as Novos_Usuarios
    , 0 as Compras_Unicas
    , 0 as Meta_01
    , 0 as Meta_02
    , 0 as Meta_03
    , 0 as Meta_04
    , 0 as Meta_05
    , coalesce(sum(qtd_eventos)) as Contagem_Eventos
    , 0 as Transacoes_Eventos
    , 0 as Receita_Eventos
    , 0 as Conversoes_Eventos
    , IF(e.ds_evento is null, null, IF(REGEXP_CONTAINS(e.ds_evento, t.eventos), COALESCE(SUM(e.qtd_eventos)), 0)) as Leads_GA
    , 0 as Leads_Totais_GA_Conversoes
    , cast(Null as string) as Source
    , cast(Null as string) as Channel
   FROM `adtail-bq.desenvolvimento.ga4_eventos` e
      LEFT JOIN `adtail-bq.desenvolvimento.clientes` t
        ON e.id_cliente = t.id_cliente
      WHERE e.dt_sessao >= date_add(date(format_datetime('%Y-%m-01',current_date())),interval -1 month)
        AND EXISTS (SELECT 1 FROM `adtail-bq.desenvolvimento.clientes` s WHERE e.id_cliente = s.id_cliente AND s.busca_gads = 1)
      GROUP BY e.dt_sessao, e.id_cliente, e.ds_campanha_sessao, e.ds_origem_midia_sessao, e.ds_evento, t.eventos;

#fim da inserção de dados
#Inicio da soma dos leads
UPDATE `desenvolvimento.BD_UNION_IM` bd
 SET bd.Leads_Totais_GA_Conversoes = bd.Conversoes+Leads_GA
 where conta_id > 0;
#inicio das informações por regras de SOURCE 
UPDATE `desenvolvimento.BD_UNION_IM` bd
 SET bd.source = 
    CASE 
    WHEN lower(SPLIT(Origem_Midia, '/')[OFFSET(0)]) like ('%facebook%') OR lower(Origem_Midia) like ('fb /%') OR lower(Origem_Midia) like ('meta /%') OR Origem = 'Facebook'
    THEN 'Facebook' 
    WHEN lower(SPLIT(Origem_Midia, '/')[OFFSET(0)]) like ('%criteo%') OR Origem = 'Criteo'
    THEN 'Criteo'
    WHEN lower(SPLIT(Origem_Midia, '/')[OFFSET(0)]) like ('%voxus%') OR Origem = 'Voxus'
    THEN 'Voxus'
    WHEN lower(SPLIT(Origem_Midia, '/')[OFFSET(0)]) like ('%blue%') OR Origem = 'Blue'
    THEN 'Blue'
    WHEN lower(SPLIT(Origem_Midia, '/')[OFFSET(0)]) like ('%pinterest%') OR Origem = 'Pinterest'
    THEN 'Pinterest'
    WHEN lower(SPLIT(Origem_Midia, '/')[OFFSET(0)]) like ('%linkedin%') OR Origem = 'LinkedIn'
    THEN 'LinkedIn'
    WHEN lower(SPLIT(Origem_Midia, '/')[OFFSET(0)]) like ('%bing%') OR Origem = 'Bing'
    THEN 'Bing'
    WHEN lower(SPLIT(Origem_Midia, '/')[OFFSET(0)]) like ('%tiktok%') OR Origem = 'TikTok'
    THEN 'TikTok'
    WHEN lower(SPLIT(Origem_Midia, '/')[OFFSET(0)]) like ('%google%') OR Origem = "Google Ads"
    THEN 'Google'
    ELSE 'Outros'
    END
 WHERE Source is null;
#Fim das informações por regras de SOURCE
#Inicio das informações por regras de CHANNEL
#Channel dos dados que que vem das API das mídias que não são analytics.
UPDATE `adtail-bq.desenvolvimento.BD_UNION_IM`
  set Channel = "Pago"
  where origem_midia is null and regexp_contains(lower(origem),"analytics") = false;
#Channel das origem / mídia que não tem "/"
update `adtail-bq.desenvolvimento.BD_UNION_IM`
  set Channel = "Outros"
  where regexp_contains(origem_midia, "/") = false;
#Channel das demais origens / mídias
UPDATE `desenvolvimento.BD_UNION_IM` bd
 SET bd.channel = 
    CASE 
    when origem_midia is null THEN "Pago"
    WHEN origem_midia = "(direct) / (none)" 
      THEN "Direto"
    WHEN origem_midia like "%/ referral" 
      THEN "Referência"
    WHEN origem_midia like "%/ organic" 
      THEN "Orgânico"
    WHEN lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%push%"
      THEN "Push Marketing"
    WHEN lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%sms%" 
      or lower(SPLIT(Origem_Midia, '/')[OFFSET(0)]) like "%sms%" 
      THEN "SMS Marketing"
    WHEN lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%mail%" 
      or lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%emkt%" 
      or lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) = "emm" 
      or lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%jornada%" 
      or lower(SPLIT(Origem_Midia, '/')[OFFSET(0)]) like "%email%"
      THEN "E-mail Marketing"
    WHEN lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%cpc%" 
      or lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%cpa%" 
      or lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%cpl%" 
      or lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%cpm%" 
      or lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%ads%" 
      or lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%ppc%" 
      or lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%display%" 
      or lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%retarget%" 
      or lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%shopping%" 
      or lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) = "dpa" 
      or lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%afiliado%" 
      or lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%affiliate%" 
      or lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%branding%" 
      or lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%paid%" 
      or lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%search%" 
      THEN "Pago"
    WHEN lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%social%" 
      or lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%post%" 
      or lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%stories%" 
      or lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%story%" 
      or lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%feed%" 
      or lower(SPLIT(Origem_Midia, '/')[OFFSET(1)]) like "%bio%"
      THEN "Social"
    WHEN investimento > 0 
      THEN "Pago"
    ELSE 'Outros'
    END
  WHERE Channel is null
#Fim das informações por regras de CHANNEL