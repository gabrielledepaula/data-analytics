/*SERVIDOR: -
AMBIENTE: -
CRIADOR: Matheus Fernandes / Modificado por Gabrielle de Paula
OBJETIVO: Dados referente a indice de falhas de estacionamentos (transits) - falhas
PRINCIPAIS INDICADORES: - OBSERVAÇÃO: -
DATA DE ALTERAÇÃO: 24/10/2023
MOTIVO DA ALTERAÇÃO: Migração para procedure e otimização de código.
*/

--informações sobre data, adesão e placa
with info as(
	SELECT 
		at.data
		,at.adesaoid 
		,at.placa 
		,case when at.datacancelamento is not null then at.datacancelamento else getdate() end as datafim
	FROM "conectcar"."cntcar_work"."tb_redshift_ciclodevida_ativacao_stage" at
)

,base_sucessos as (
	SELECT 
		trunc(intime) as Data
		,Count(a.id)::int as sucessos 
		,0::int as erro
		,case when d.anoreferencia is not null then 'Itaú' else C.agregado end as agregado
		,a.falha
	FROM cntcar_work.tb_bam_dbo_indicedefalhasestacionamento_stage a  
	left join info b on a.placa = b.placa and a.intime >= b.data and a.intime < b.datafim
	left join cntcar_work.tb_historico_itau_cobranded_stage d on b.adesaoid = d.adesaoid and cast(anoreferencia || '-' || mesreferencia || '-' || '01' as date) = date_trunc('month', a.intime) and checkfaturamento = 1
	left join cntcar_work.tb_redshift_ciclodevida_portifolio_stage c on c.adesaoid = b.adesaoid
	where a.intime >= '20220101' and a.falha = 0 
	group by 
	trunc(intime)
	,case when d.anoreferencia is not null then 'Itaú' else C.agregado end
	,a.falha
)

,base_falhas as (
	SELECT 
		trunc(intime) as data
		,0::int as sucessos
		,Count (a.id)::int as erro
		,case when d.anoreferencia is not null then 'Itaú' else C.agregado end as agregado
		,a.falha
	FROM cntcar_work.tb_bam_dbo_indicedefalhasestacionamento_stage a  
	left join info b on a.placa = b.placa and a.intime >= b.data and a.intime < b.datafim
	left join cntcar_work.tb_historico_itau_cobranded_stage d on b.adesaoid = d.adesaoid and cast(anoreferencia || '-' || mesreferencia || '-' || '01' as date) = date_trunc('month', a.intime) and checkfaturamento = 1
	left join cntcar_work.tb_redshift_ciclodevida_portifolio_stage c on c.adesaoid = b.adesaoid
	where a.intime >= '20220101' and a.falha = 1 
	group by 
	trunc(intime), 
	case when d.anoreferencia is not null then 'Itaú' else C.agregado end,
	a.falha
)
select * from base_sucessos
union
select * from base_falhas