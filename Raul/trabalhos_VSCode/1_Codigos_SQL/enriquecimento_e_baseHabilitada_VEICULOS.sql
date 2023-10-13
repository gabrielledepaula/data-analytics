----------------------------------------------------------
--- SELECIONANDO SOMENTE CLIENTES HABILITADOS CONECTCAR
----------------------------------------------------------

drop table if exists #documentosHabilitados2023;
select 
left(datahabilitada, 4) || right(left(a.datahabilitada, 7),2) :: int as anomes,
    b.nomeplano,
    a.agregado,
    b.documento
into #documentosHabilitados2023
from cntcar_work.vw_redshift_ciclodevida_basehabilitada_stage as a
left join cntcar_work.vw_redshift_ciclodevida_ativacao as b 
on a.adesaoid = b.adesaoid
where anomes >= 20231
and agregado not in ('Itaú');


DROP TABLE IF EXISTS #documentosHabilitados2023_ultimoMes;
SELECT DISTINCT
    documento,
    max(anomes) as ultimoMesHabilitado
into #documentosHabilitados2023_ultimoMes
from #documentosHabilitados2023
group by 1
;


DROP TABLE IF EXISTS  #documentosHabilitados2023_v1;
SELECT DISTINCT
    a.documento,
    a.ultimoMesHabilitado,
    b.agregado 
into #documentosHabilitados2023_v1
from #documentosHabilitados2023_ultimoMes as a
left join #documentosHabilitados2023 as b
    on a.documento = b.documento and a.ultimoMesHabilitado = b.anomes
order by 2,3,1 desc
;


DROP TABLE IF EXISTS  #documentosHabilitados2023_v2;
select 
documento,
ultimoMesHabilitado,
agregado,
ROW_NUMBER () OVER
(
 PARTITION BY documento
ORDER BY ultimoMesHabilitado,documento, agregado
) as rownum
into #documentosHabilitados2023_v2
from #documentosHabilitados2023_v1;


--REMOVENDO CENARIOS ONDE O MESMO DOCUMENTO POSSUA 1+ AGREGADOS
DROP TABLE IF EXISTS  #documentosHabilitados2023_v3;
select 
    documento,
    ultimomeshabilitado,
    agregado,
    rownum as flag_habilitado
into #documentosHabilitados2023_v3
from #documentosHabilitados2023_v2
where rownum = 1
order by 2 desc;

DROP TABLE IF EXISTS #documentosHabilitados2023_v4;
select 
documento,
ultimoMesHabilitado,
agregado,
ROW_NUMBER () OVER
(
ORDER BY ultimoMesHabilitado
) as id_enriquecimento
into #documentosHabilitados2023_v4
from #documentosHabilitados2023_v3;

DROP TABLE IF EXISTS #documentosHabilitados2023;
DROP TABLE IF EXISTS #documentosHabilitados2023_v1;
DROP TABLE IF EXISTS #documentosHabilitados2023_v2;
DROP TABLE IF EXISTS #documentosHabilitados2023_v3;
DROP TABLE IF EXISTS #documentosHabilitados2023_ultimoMes;

------------------------------------------
----------- GRUPO PF HABILITADOS 2023 ----
----------------------X-------------------
------------------ VEICULOS --------------
------------------------------------------
DROP TABLE IF EXISTS #VEICULOS_FULL_TRAT;
select distinct
*,
substring(regexp_replace(DOCUMENTO, '[^0-9]+', '0'),1,12)*1 as DOC_NUM
INTO #VEICULOS_FULL_TRAT
from  "conectcar"."analytics_work"."tbl_veiculo";

DROP TABLE IF EXISTS #VEICULOS_FULL;
SELECT DISTINCT
    A.documento,
    A.placa,
    A.ano,
    A.marca,
    A.modelo,
    A.tipo,
    A.valorveiculofipe,
    A.tipocomustivelfipe,
    B.ultimoMesHabilitado
INTO #VEICULOS_FULL
FROM #VEICULOS_FULL_TRAT AS A
LEFT JOIN #documentosHabilitados2023_v4 AS B
ON (A.DOC_NUM*1) = B.DOCUMENTO*1 
WHERE  ultimoMesHabilitado IS NOT NULL
ORDER BY ultimoMesHabilitado, A.DOCUMENTO, A.PLACA DESC
;


DROP TABLE IF EXISTS #VEICULOS_HABILITADA_2023;
SELECT DISTINCT 
    documento,
    placa,
    ano,
    marca,
    modelo,
    tipo,
    valorveiculofipe,
    tipocomustivelfipe,
    ultimoMesHabilitado,
ROW_NUMBER () OVER
(
    PARTITION BY placa
    ORDER BY ultimoMesHabilitado, DOCUMENTO, PLACA DESC
) as rownum
into #VEICULOS_HABILITADA_2023
from #VEICULOS_FULL;

--REMOVENDO PLACAS COM MAIS DE 1+ DOCUMENTOS
DROP TABLE IF EXISTS #VEICULOS_HABILITADA_2023_v1;
SELECT DISTINCT
    documento,
    placa,
    ano,
    marca,
    modelo,
    tipo,
    valorveiculofipe,
    tipocomustivelfipe,
    ultimoMesHabilitado
INTO #VEICULOS_HABILITADA_2023_v1
FROM #VEICULOS_HABILITADA_2023
WHERE rownum = 1;

DROP TABLE IF EXISTS #VEICULOS_HABILITADA_2023_v2;
SELECT DISTINCT 
    documento,
    placa,
    ano,
    marca,
    modelo,
    tipo,
    valorveiculofipe,
    tipocomustivelfipe,
    ultimoMesHabilitado,
ROW_NUMBER () OVER
(
ORDER BY ultimoMesHabilitado, placa, documento
) as id_enriquecimento
into #VEICULOS_HABILITADA_2023_v2
from #VEICULOS_HABILITADA_2023_v1;

DROP TABLE IF EXISTS #VEICULOS_HABILITADA_2023;
DROP TABLE IF EXISTS #VEICULOS_HABILITADA_2023_v1;
DROP TABLE IF EXISTS #VEICULOS_FULL;


--EXPORTANDO BASES ANALITICAS 

SELECT DISTINCT * 
FROM #VEICULOS_HABILITADA_2023_v2
WHERE id_enriquecimento <= 700000
ORDER BY id_enriquecimento asc ;

SELECT DISTINCT * 
FROM #VEICULOS_HABILITADA_2023_v2
WHERE id_enriquecimento > 700000
ORDER BY id_enriquecimento asc ;


