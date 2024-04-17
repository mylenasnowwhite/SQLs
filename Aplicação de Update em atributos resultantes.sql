UPDATE caminho-tabela /*definição nm_source*/
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
        	OR  lower(SUBSTRING_INDEX(nm_origemmidia, '/', 1)) like ('%adwords%')
            THEN 'Google'
        ELSE 'Outros'
    END
	WHERE nm_source = '' OR nm_source IS NULL;
#Fim das informações por regras de nm_source
#Inicio das informações por regra de nm_channel
UPDATE caminho-tabela /*definição nm_channel*/
	SET nm_channel = 'Outros'
	WHERE nm_origemmidia NOT REGEXP '/';
UPDATE caminho-tabela /*definição nm_channel*/
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
				 LOWER(SUBSTRING_INDEX(nm_origemmidia, '/', -1)) LIKE '%rev%' OR
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