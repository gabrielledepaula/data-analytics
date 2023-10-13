-------------------------------
-- BASES LEGADAS BKOxSERASA
-------------------------------


DROP TABLE IF EXISTS #tbl_cadastralpf1;
select 
a.*,
LOWER(NULLIF(coalesce(a.faixaderendaserasa, b.faixa_renda_enr),'')) as faixaderendaserasa2,
LOWER(NULLIF(coalesce(a.ocupacaoserasa, b.cbo_descricao_enr),''))  as ocupacaoserasa2,
CASE
WHEN b.status_cpf in ('REGULAR', 'PENDENTE DE REGULARIZACAO','TITULAR FALECIDO','SUSPENSA','CANCELADA')
THEN LOWER(coalesce(a.statuscpfserasa, b.status_cpf))
ELSE NULL
END statuscpfserasa2,
LOWER(NULLIF(coalesce(a.cepserasa, b.cep1_padr),''))  as cepserasa2
INTO #tbl_cadastralpf1 
from analytics_work.tbl_cadastralpf as a
left join analytics_work.tbl_primeirahigienizacaopfFULL as b
on a.documento = b.cpf;


ALTER TABLE #tbl_cadastralpf1 DROP COLUMN faixaderendaserasa;
ALTER TABLE #tbl_cadastralpf1 DROP COLUMN ocupacaoserasa;
ALTER TABLE #tbl_cadastralpf1 DROP COLUMN statuscpfserasa;
ALTER TABLE #tbl_cadastralpf1 DROP COLUMN cepserasa;

ALTER TABLE #tbl_cadastralpf1 RENAME COLUMN faixaderendaserasa2 TO faixaderendaserasa;
ALTER TABLE #tbl_cadastralpf1 RENAME COLUMN ocupacaoserasa2 TO ocupacaoserasa;
ALTER TABLE #tbl_cadastralpf1 RENAME COLUMN statuscpfserasa2 TO statuscpfserasa;
ALTER TABLE #tbl_cadastralpf1 RENAME COLUMN cepserasa2 TO cepserasa;

DROP TABLE IF EXISTS #tbl_cadastralpf;
SELECT
    documento,
    clienteid,
    nome,
    datanascimento,
    sexo,
    estadocivil,
    nomemae,
    enderecoid,
    cep,
    uf,
    cidade,
    bairro,
    logradouro,
    dddcelular,
    celular,
    email,
    faixaderendaserasa,
    ocupacaoserasa,
    statuscpfserasa,
    escolaridade,
    cepserasa,
    rendacepibge,
    pedagiopreferido,
    segmentoestacionamentopreferido,
    estacionamentopreferido,
    meiodepagamentopreferido,
    qtdadesoes,
    latitude,
    longitude,
    bancoemissorcartaodecreditoatual,
    bancoemissormaisutilizado
INTO #tbl_cadastralpf
FROM #tbl_cadastralpf1
;

DROP TABLE IF EXISTS #tbl_cadastralpf1;

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
ORDER BY ultimoMesHabilitado, documento desc
) as id_enriquecimento
into #documentosHabilitados2023_v4
from #documentosHabilitados2023_v3;

DROP TABLE IF EXISTS #documentosHabilitados2023;
DROP TABLE IF EXISTS #documentosHabilitados2023_v1;
DROP TABLE IF EXISTS #documentosHabilitados2023_v2;
DROP TABLE IF EXISTS #documentosHabilitados2023_v3;
DROP TABLE IF EXISTS #documentosHabilitados2023_ultimoMes;


-----------------PRINCIPAIS TABELAS

-- #documentosHabilitados2023_v4
--- #tbl_cadastralpf


------------------------------------------
----------- GRUPO PF HABILITADOS 2023 ----
------------------- 1ª ONDA --------------
------------------------------------------

DROP TABLE IF EXISTS #ONDA_1_ENRIQUECIMENTO;
SELECT DISTINCT
    a.documento,
    a.faixaderendaserasa,
    a.ocupacaoserasa,
    a.estadocivil,
    a.escolaridade,
    a.datanascimento,
    a.sexo,
    a.statuscpfserasa,
    b.ultimomeshabilitado,
    b.agregado,
    b.id_enriquecimento
INTO #ONDA_1_ENRIQUECIMENTO
FROM #tbl_cadastralpf as a 
LEFT JOIN #documentosHabilitados2023_v4 as b
ON a.documento = b.documento
where ultimomeshabilitado is not null
;

select * from #ONDA_1_ENRIQUECIMENTO;


