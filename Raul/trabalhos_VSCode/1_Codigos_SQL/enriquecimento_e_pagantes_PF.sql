------------------------------------------------------
----------- DEFININDO CLIENTES PAGANTES DE MENSALIDADE
-------------------------------------------------------


DROP TABLE IF EXISTS #pagantesmensalidades;
with adesoesitau as (

	select 
		cast(c.anoreferencia || '-' || c.mesreferencia || '-' || '01' as date) as datareferencia,
		c.adesaoid
	from 
		cntcar_work.tb_historico_itau_cobranded_stage c 
    where
		c.checkfaturamento = 1),

basehabilitada as (

	select distinct
		bh.adesaoid
	from
		cntcar_work.vw_redshift_ciclodevida_basehabilitada_stage as bh
	left outer join
		adesoesitau as ai 
	on
		ai.adesaoid = bh.adesaoid 
		and ai.datareferencia = date_trunc('month', bh.datahabilitada)
	where 
		bh.datahabilitada >= '20230101'
		and ai.adesaoid is null),

portifolio as (

	select 
		p.adesaoid,
		p.agregado,
		p.detalhado,
		p.plano
	from 
		cntcar_work.tb_redshift_ciclodevida_portifolio_stage as p
	where 
		p.adesaoid in (select bh.adesaoid from basehabilitada as bh)),

ativacao as (

	select
		a.adesaoid,
		a.documento,
		a.clienteid,
		a."data" as dataadesao,
		a.tagid,
		a.nomeplano
	from 
		cntcar_work.tb_redshift_ciclodevida_ativacao_stage as a
	where
		a.adesaoid in (select bh.adesaoid from basehabilitada as bh)),
		
cliente as (

	select
		cli.clienteid,
		case when (cli.pessoafisica = true) then 'PF' else 'PJ' end as tipopessoa
	from 
		cntcar_work.tb_conectcar_dbo_cliente_stage as cli
	where 
		cli.clienteid in (select a.clienteid from ativacao as a)),
		
mensalidade as (

	select distinct
		at.PrimeiraADesaoId,
		at.adesaoid,
	    ti.TagId, 
	    ti.TagsIsencaoId, 
	    ti.IsencaoClienteId, 
	    ic.DataInicial, 
	    ic.DataFinal,
	    case when ti.Porcentagemdesconto = 0 and lower(ic.Descricao) similar to '%isencao%|%isenção%|%isen%|%insen%|%vitalicio%|%vitalíci%' then 'Isenção'
	    	when ti.Porcentagemdesconto = 100  then 'Isenção'
	    		when ti.Porcentagemdesconto >0 and ti.Porcentagemdesconto < 100  then 'Oferta' else 'N/A'
		end as tipo_retencao
	from
		cntcar_work.tb_conectcar_dbo_tagsisencao_stage as ti 
	inner join
		cntcar_work.tb_conectcar_dbo_isencaoCliente_stage as ic 
	on
		ti.IsencaoClienteId = ic.IsencaoClienteId 
	inner join
		cntcar_work.tb_redshift_ciclodevida_ativacao_stage as at 
	on
		at.tagId = ti.TagId 
    	and at.documento <> '16670085000155'
    	and lower(at.nomeplano) similar to '%completo%|fiat|flex|rodovia|urbano'
	where
		ti.Porcentagemdesconto = 100 or ti.Porcentagemdesconto = 0
		and tipo_retencao = 'Isenção'
		and cast(ic.DataFinal as date) >= '20230101'),

tratativa as (

	select distinct
		min(a.dataadesao) as dataprimeiraadesao,
		a.documento,
		sum(case when (m.adesaoid is null or cast(m.datafinal as date) < current_date) then 1 else 0 end) flagpagantes
	from 
		basehabilitada as bh
	left outer join
		portifolio as p
	on
		p.adesaoid = bh.adesaoid
	left outer join 
		ativacao as a
	on
		a.adesaoid = bh.adesaoid
	left outer join 
		cliente as c 
	on
		c.clienteid = a.clienteid
	left outer join 
		mensalidade as m 
	on
		m.tagid = a.tagid
	where 
		upper(p.plano) in ('COMPLETO','FIAT','FLEX','RODOVIA','URBANO')
	group by
		2),
		
tratativa_1 as (

	select distinct
		a.dataadesao,
		a.adesaoid,
		a.documento,
		p.plano
	from 
		basehabilitada as bh
	left outer join
		portifolio as p
	on
		p.adesaoid = bh.adesaoid
	left outer join 
		ativacao as a
	on
		a.adesaoid = bh.adesaoid
	left outer join 
		cliente as c 
	on
		c.clienteid = a.clienteid
	left outer join 
		mensalidade as m 
	on
		m.tagid = a.tagid
	where 
		upper(p.plano) in ('COMPLETO','FIAT','FLEX','RODOVIA','URBANO'))

select
	t.dataprimeiraadesao,
	t.documento,
	(select top 1 t1.plano from tratativa_1 as t1 where t1.documento = t.documento order by t1.dataadesao desc) as ultimoplano,
	case when (flagpagantes > 0) then 1 else 0 end flagpagantes
     into #pagantesmensalidades
from 
	tratativa as t
order by
	1
asc
;



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

-------------------------------------------------
--- SELECIONANDO SOMENTE CLIENTES PAGANTES
---- 1º GRUPO DE CLIENTES PF PARA ENRIQUECIMENTO
-----------------------------------------------------
DROP TABLE IF EXISTS #GRUPO_1_PF;
SELECT DISTINCT
    a.documento,
    to_char(b.dataprimeiraadesao, 'YYYY-MM'),
    b.flagpagantes,
    b.ultimoplano,
    a.faixaderendaserasa,
    a.ocupacaoserasa,
    a.statuscpfserasa,
    a.cepserasa
INTO #GRUPO_1_PF
FROM #tbl_cadastralpf as A
left join #pagantesmensalidades as b
on a.documento = b.documento
where flagpagantes = 1;


select * from #GRUPO_1_PF;

-------------------------------------------------
--- SELECIONANDO SOMENTE CLIENTES PAGANTES
---- 2º GRUPO DE CLIENTES PF PARA ENRIQUECIMENTO
-----------------------------------------------------
DROP TABLE IF EXISTS #GRUPO_2_PF;
SELECT DISTINCT
    a.documento,
    to_char(b.dataprimeiraadesao, 'YYYY-MM'),
    b.flagpagantes,
    b.ultimoplano,
    a.faixaderendaserasa,
    a.ocupacaoserasa,
    a.statuscpfserasa,
    a.cepserasa
INTO #GRUPO_2_PF
FROM #tbl_cadastralpf as A
left join #pagantesmensalidades as b
on a.documento = b.documento;

DROP TABLE IF EXISTS #GRUPO_2_PF_v2;
SELECT DISTINCT
*
into #GRUPO_2_PF_v2
FROM #GRUPO_2_PF 
WHERE faixaderendaserasa IS NULL OR ocupacaoserasa IS NULL OR statuscpfserasa IS NULL OR cepserasa IS NULL
AND flagpagantes <> 1;

DROP TABLE IF EXISTS #GRUPO_2_PF_v3;
SELECT 
DISTINCT
documento,
faixaderendaserasa,
ocupacaoserasa
into #GRUPO_2_PF_v3
FROM #GRUPO_2_PF_v2
where faixaderendaserasa IS NULL AND ocupacaoserasa IS null
order by DOCUMENTO ASC
LIMIT 642927;


--------------------------------------------------
--- SELECIONANDO SOMENTE CLIENTES PAGANTES
---- 3º GRUPO DE CLIENTES PF PARA ENRIQUECIMENTO
---------------------------------------------------

select * from #GRUPO_1_PF;
select * from #GRUPO_2_PF_v3;

drop table if exists #GRUPO_3_PF;
select 
	a.documento,
    a.faixaderendaserasa,
    a.ocupacaoserasa,
    a.statuscpfserasa,
    a.cepserasa,
    b.documento as grupo1,
    c.documento as grupo2
into #GRUPO_3_PF
    from #tbl_cadastralpf as a
    left join #GRUPO_1_PF as b 
    	on a.documento = b.documento
    left join #GRUPO_2_PF_v3 as c
		on a.documento = c.documento
WHERE a.faixaderendaserasa IS NULL OR a.ocupacaoserasa IS NULL OR a.statuscpfserasa IS NULL OR a.cepserasa IS NULL