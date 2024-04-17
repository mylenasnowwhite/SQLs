#Limpar a tabela projeto.tabela_bdg
truncate table  projeto.tabela_bdg;

#Inserir os dados tratados
insert into projeto.tabela_bdg
Select distinct * from (
with BD_BQ as (
  #Junção dos dados Meta Ads e Google Ads
    with BQ_Midia as 
      (
       SELECT  
          dt_campanha,
          nm_midia,
          nm_campanha,
          part_nm_campanha,
          nm_anuncio,
          sum(cast(replace(cast(vl_custo as string),".","") as int64))/100 as vl_custo,
          sum(qtd_impressoes) as qtd_impressoes,
          sum(qtd_cliques) as qtd_cliques
          FROM `adtail-bq.projeto.tabela_bd_aws` 
          group by 1,2,3,4,5
      ),
  #Definição do conjunto de produtos  
    Df_Produtos as
      (
        select #tabela_controledeprodutos sheets relatório tabela aba CONTROLE DE PRODUTOS
          nm_produto,
          set_produto,
          replace(TRIM(upper(nm_campanha)),"|","+") as nm_campanha,
          trim(if(trim(REGEXP_EXTRACT(replace(TRIM(upper(nm_campanha)),"|","+"), r'EXTRA[^+]+')) is null, CONCAT(SPLIT(replace(TRIM(upper(nm_campanha)),"|","+"), '+')[OFFSET(2)], '+', SPLIT(replace(TRIM(upper(nm_campanha)),"|","+"), '+')[OFFSET(3)]) , trim(REGEXP_EXTRACT(replace(TRIM(upper(nm_campanha)),"|","+"), r'EXTRA[^+]+')))) as part_nm_campanha,
          trim(upper(concat("AT - ",nm_midia))) as nm_midia
          from
          projeto.tabela_controledeprodutos
      ),
  #Tratamento de Dados de Produtos a nível de campanha
    BQ_midiaxproduto  as
      (
        select 
        a.*, 
        upper(b.set_produto) as set_produto
        from BQ_Midia a
        left join(select distinct set_produto,part_nm_campanha from Df_Produtos) b on regexp_contains(b.part_nm_campanha,a.part_nm_campanha)
      ),
  #Tratamento de set_produto null
    BQ_DADOS_TRATADOS_PROD as
      (
        Select
        a.dt_campanha,
        a.nm_midia,
        "" as ds_status,
        ifnull(a.set_produto,b.set_produto) as set_produto,
        concat(replace(a.nm_campanha,"+","|")," - ",ifnull(ifnull(a.set_produto,b.set_produto), 'vazio')) as key,
        "" as ds_produto,
        replace(a.nm_campanha,"+","|") as nm_campanha,
        replace(a.nm_anuncio,"+","|") as nm_anuncio,
        sum(a.vl_custo) as vl_custo,
        sum(a.qtd_impressoes) as qtd_impressoes,
        sum(a.qtd_cliques) as qtd_cliques,
        0 as qtd_lead
        from BQ_midiaxproduto a
        left join (select distinct set_produto,part_nm_campanha from Df_Produtos) b on a.part_nm_campanha = b.part_nm_campanha
        group by 1,2,3,4,5,6,7,8
      ),
  #Selecionando as campanhas que possuem somente um set_produto  
    BQ_CAMPANHA_SETPRODUTOUNICO as (

          Select distinct
          a.nm_campanha,
          a.set_produto,
          if(regexp_contains(a.nm_campanha, a.set_produto),True,False) as validacao,
          b.entrada,
          concat(a.nm_campanha," - ",a.set_produto) as key
          from
          BQ_DADOS_TRATADOS_PROD a
          left join (
          Select 
            nm_campanha,
            max(entrada) as entrada
            from(
                  Select distinct 
                   set_produto,
                   nm_campanha,
                   ROW_NUMBER() OVER (PARTITION BY  nm_campanha ORDER BY min(set_produto)) as entrada
                   from BQ_DADOS_TRATADOS_PROD
                   group by 1,2
                )
            group by 1
    ) b on a.nm_campanha = b.nm_campanha
),
# Remoção de campanhas com mais de um tipo de set_produto
    BQ_DADOS as (
        select distinct * from BQ_DADOS_TRATADOS_PROD
        where key in (
        select distinct concat(nm_campanha," - ",ifnull(set_produto, 'vazio')) as  key from BQ_CAMPANHA_SETPRODUTOUNICO where entrada = 1
        union all
        select distinct concat(nm_campanha," - ",if(validacao = true, set_produto, 'ONGOING')) as key from BQ_CAMPANHA_SETPRODUTOUNICO where entrada = 2 and key not in (select distinct concat(nm_campanha," - ",'ONGOING') as key from BQ_CAMPANHA_SETPRODUTOUNICO where validacao = true and entrada = 2))
    ),
 #----------------------------//----------------------------- Dados tratados via ferramenta
 BD_ferramenta_LEADS as (
select *, ifnull(ds_avaliacao_1,'SEM AVALIAÇÃO') as ds_avaliacao
FROM `adtail-bq.tabela.ferramenta_leads` a
where 
            date(format_datetime('%Y-%m-01', dt_entrada)) >= date_add(date(format_datetime('%Y-%m-01',current_date())),interval -5 month) and
            regexp_contains(upper(ds_iferramentaliaria),'tabela') and not regexp_contains(upper(ds_produto),'tabela2')
),
 BD_ferramenta_TRATADO as (
select a.*, 
ifnull((case
            when upper(ds_veiculo) like "FACEBOOK_SELLER" then "FACEBOOK_SELLER"
            when upper(ds_veiculo) like "%GOOGLE%" and (regexp_contains(upper(ds_origem_1),"CPC|DISPLAY") or regexp_contains(upper(ds_veiculo),"CPC|DISPLAY")) then 'AT - GOOGLE'
            when upper(ds_veiculo) like "%GOOGLE%" and (regexp_contains(upper(ds_midia),"CPC|DISPLAY")) then 'AT - GOOGLE'
            when upper(ds_veiculo) like "%GOOGLE%" and regexp_contains(upper(ds_origem_contato),"ADTAIL META") then 'AT - GOOGLE'
            when (upper(ds_veiculo) like "%FACEBOOK CTW%" or upper(ds_veiculo) like "%FACEBOOK_LEAD%") and upper(ds_fila) like "%WHATSAPP%" then 'FACEBOOK CTW'
            when (upper(ds_veiculo) like "%FACEBOOK CTW%" or upper(ds_veiculo) like "%FACEBOOK_LEAD%") and upper(ds_fila) NOT like "%WHATSAPP%" then 'AT - FACEBOOK'
            when (upper(ds_veiculo) like "%FACEBOOK_LEAD%" or (regexp_contains(upper(ds_veiculo),"FORMULÁRIO|FORMULARIO") and upper(ds_origem_contato) like "%ADTAIL%")) then 'AT - FACEBOOK'
            when upper(ds_veiculo) like "%GOOGLE%" and upper(ds_origem_1) like "%ORGANIC%" then "GOOGLE ORGÂNICO"
            when upper(ds_veiculo) like "%GOOGLE%" and (regexp_contains(upper(ds_midia),"ORGANIC")) then 'GOOGLE ORGÂNICO'
            when upper(ds_veiculo) like "%GOOGLE%" and upper(ds_origem_1) is null then "GOOGLE OUTROS"
            when upper(ds_veiculo) like "%VINTAGE%" or upper(ds_origem_contato) like "%VINTAGE%"  then "LP-VINTAGE" 
            when upper(ds_veiculo) like "%PRODUTO%" then "LP-PRODUTO"
            when upper(ds_veiculo) like "IMOVELWEB" then "IMOVEL WEB"
            else replace(replace(case when 
            (
              ifnull(case
    when upper(ds_veiculo) like "%VINTAGE%" or upper(ds_origem_contato) like "%VINTAGE%"  then "LP-VINTAGE" 
    when upper(ds_veiculo) like "%PRODUTO%" then "LP-PRODUTO"
    when upper(ds_veiculo) like "IMOVELWEB" then "IMOVEL WEB"
    when upper(ds_veiculo) like "FORMULÁRIO" or upper(ds_veiculo) like "FORMULARIO" or upper(ds_veiculo) like "CHAT" then 
    ( case
      when upper(ds_veiculo) like "%VINTAGE%" or upper(ds_origem_contato) like "%VINTAGE%"  then "LP-VINTAGE" 
      when upper(ds_veiculo) like "%PRODUTO%" then "LP-PRODUTO"
      when upper(ds_origem_contato) like "IMOVELWEB" then "IMOVEL WEB"
    when upper(ds_origem_contato) like "CHAT" and upper(ds_meio) like "CHAT" and upper(ds_veiculo) like "LIVE" then "LIVE" 
 else upper(ds_origem_contato)
      end
    )
    else     ( case
      when upper(ds_veiculo) like "%VINTAGE%" or upper(ds_origem_contato) like "%VINTAGE%"  then "LP-VINTAGE" 
      when upper(ds_veiculo) like "%PRODUTO%" then "LP-PRODUTO"
      when upper(ds_origem_contato) like "IMOVELWEB" then "IMOVEL WEB"
    when upper(ds_origem_contato) like "CHAT" and upper(ds_meio) like "CHAT" and upper(ds_veiculo) like "LIVE" then "LIVE" 
 else upper(ds_origem_contato)
      end
    )
  end, ds_veiculo)
            ) like "%|%" then split(TRIM(SPLIT(
               ifnull(case
    when upper(ds_veiculo) like "%VINTAGE%" or upper(ds_origem_contato) like "%VINTAGE%"  then "LP-VINTAGE" 
    when upper(ds_veiculo) like "%PRODUTO%" then "LP-PRODUTO"
    when upper(ds_veiculo) like "IMOVELWEB" then "IMOVEL WEB"
    when upper(ds_veiculo) like "FORMULÁRIO" or upper(ds_veiculo) like "FORMULARIO" or upper(ds_veiculo) like "CHAT" then 
        ( case
      when upper(ds_veiculo) like "%VINTAGE%" or upper(ds_origem_contato) like "%VINTAGE%"  then "LP-VINTAGE" 
      when upper(ds_veiculo) like "%PRODUTO%" then "LP-PRODUTO"
      when upper(ds_origem_contato) like "IMOVELWEB" then "IMOVEL WEB"
    when upper(ds_origem_contato) like "CHAT" and upper(ds_meio) like "CHAT" and upper(ds_veiculo) like "LIVE" then "LIVE" 
 else upper(ds_origem_contato)
      end
    )
    else
        ( case
      when upper(ds_veiculo) like "%VINTAGE%" or upper(ds_origem_contato) like "%VINTAGE%"  then "LP-VINTAGE" 
      when upper(ds_veiculo) like "%PRODUTO%" then "LP-PRODUTO"
      when upper(ds_origem_contato) like "IMOVELWEB" then "IMOVEL WEB"
    when upper(ds_origem_contato) like "CHAT" and upper(ds_meio) like "CHAT" and upper(ds_veiculo) like "LIVE" then "LIVE" 
 else upper(ds_origem_contato)
      end
    )
  end, ds_veiculo)
              , '|')[OFFSET(0)]), ',')[OFFSET(0)] else split(TRIM(SPLIT(
                 ifnull(case
    when upper(ds_veiculo) like "%VINTAGE%" or upper(ds_origem_contato) like "%VINTAGE%"  then "LP-VINTAGE" 
    when upper(ds_veiculo) like "%PRODUTO%" then "LP-PRODUTO"
    when upper(ds_veiculo) like "IMOVELWEB" then "IMOVEL WEB"
    when upper(ds_veiculo) like "FORMULÁRIO" or upper(ds_veiculo) like "FORMULARIO" or upper(ds_veiculo) like "CHAT" then 
        ( case
      when upper(ds_veiculo) like "%VINTAGE%" or upper(ds_origem_contato) like "%VINTAGE%"  then "LP-VINTAGE" 
      when upper(ds_veiculo) like "%PRODUTO%" then "LP-PRODUTO"
      when upper(ds_origem_contato) like "IMOVELWEB" then "IMOVEL WEB"
    when upper(ds_origem_contato) like "CHAT" and upper(ds_meio) like "CHAT" and upper(ds_veiculo) like "LIVE" then "LIVE" 
 else upper(ds_origem_contato)
      end
    )
    else 
        ( case
      when upper(ds_veiculo) like "%VINTAGE%" or upper(ds_origem_contato) like "%VINTAGE%"  then "LP-VINTAGE" 
      when upper(ds_veiculo) like "%PRODUTO%" then "LP-PRODUTO"
      when upper(ds_origem_contato) like "IMOVELWEB" then "IMOVEL WEB"
    when upper(ds_origem_contato) like "CHAT" and upper(ds_meio) like "CHAT" and upper(ds_veiculo) like "LIVE" then "LIVE" 
 else upper(ds_origem_contato)
      end
    )
  end, ds_veiculo)
                , '|')[OFFSET(0)]), ',')[OFFSET(0)] end,")",""),"(","")
            end),'DIRECT')  as nm_midia,

          case
            when regexp_contains(ifnull(UPPER(ds_avaliacao),"SEM AVALIAÇÃO"),"TESTE|TROTE") then "NÃO CONSIDERADOS"
            when regexp_contains(ifnull(UPPER(ds_avaliacao),"SEM AVALIAÇÃO"),"COM INTERESSE|NENHUM PRODUTO ATENDE|CIO DA PROCURA POR IM") then "QUALIFICADOS"
            when regexp_contains(ifnull(UPPER(ds_avaliacao),"SEM AVALIAÇÃO"),"SEM AVALIAÇÃO") then"SEM AVALIAÇÃO"
            when regexp_contains(ifnull(UPPER(ds_avaliacao),"SEM AVALIAÇÃO"),"SEM RESPOSTA|EM BUSCA|ERRADO|JÁ É|NÃO É|SEM INTERESSE|SEM RENDA") then "DESQUALIFICADO"
            else "AVALIADOS"
            end as ds_status,
            upper(ifnull(b.set_produto, a.ds_produto)) as set_produto,            
            upper(ds_produto) as ds_produtos
from BD_ferramenta_LEADS a
          left join 
            (select distinct
            trim(
              replace(
                replace(
                  replace(
                    replace(
                      upper(nm_produto)
                    ," ,",",")
                  ," , ",",")
                ,", ",",")
              ,",","|")
            ) as nm_produto
            ,upper(set_produto) as set_produto
            From  `adtail-bq.projeto.tabela_controledeprodutos`) b on regexp_contains(upper(a.ds_produto),upper(b.nm_produto))
),
BD_ferramenta_LEAD_TRATDO_CONTAGEM as (
select distinct *, 1 as qtd_leads
from BD_ferramenta_TRATADO)

#--- PREPARAÇÃO DADOS AWS ---#
Select 
dt_campanha, 
nm_midia, 
ds_status, 
set_produto, 
ds_produto,
nm_campanha,
nm_anuncio, 
vl_custo,
qtd_impressoes,
qtd_cliques, 
qtd_lead  from BQ_DADOS
UNION ALL
#--- PREPARAÇÃO DADOS ferramenta ---#
select 
dt_entrada,
nm_midia,
ds_status,
set_produto,
ds_produtos, 
null as nm_campanha, 
null as nm_anuncio, 
0 as vl_custo, 
0 as qtd_impressoes, 
0 as qtd_cliques,  
sum(qtd_leads) as qtd_leads 
from BD_ferramenta_LEAD_TRATDO_CONTAGEM 
group by 1,2,3,4,5)

Select 
  dt_campanha,
  nm_midia,
  set_produto,
  ds_produto,
  nm_campanha,
  nm_anuncio,
  sum(vl_custo) as vl_custo,
  cast(sum(qtd_impressoes) as int64) as qtd_impressoes,
  sum(qtd_cliques) as qtd_cliques,
  sum(if(ds_status = "NÃO CONSIDERADOS", 0, qtd_lead)) as leads_totais,
  sum(if(ds_status = "QUALIFICADOS", qtd_lead, 0)) as leads_qualificados,
  sum(if(ds_status = "DESQUALIFICADO", qtd_lead, 0)) as leads_desqualificados,
  sum(if(regexp_contains(ds_status,"QUALI"), qtd_lead,0)) as leads_avaliados,
  sum(if(ds_status = "NÃO CONSIDERADOS", qtd_lead, 0)) as leads_naoconsiderados,
  sum(if(ds_status = "SEM AVALIAÇÃO", qtd_lead, 0)) as leads_semavaliacao
  from BD_BQ
  group by 
  dt_campanha,
  nm_midia,
  set_produto,
  ds_produto,
  nm_campanha,
  nm_anuncio
)