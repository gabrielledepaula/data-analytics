/*SERVIDOR: -
AMBIENTE: -
CRIADOR: Matheus Fernandes / Modificado por Gabrielle de Paula
OBJETIVO: Dados referente a indice de falhas de estacionamentos (transits) - Sucessos
PRINCIPAIS INDICADORES: - OBSERVAÇÃO: -
DATA DE ALTERAÇÃO: 24/10/2023
MOTIVO DA ALTERAÇÃO: Migração para procedure e otimização de código.
*/

with info as (
	SELECT 
		at.data 
		,at.adesaoid
		,at.placa
		,case when at.datacancelamento is not null then at.datacancelamento 
		else getdate() end as datafim
	FROM "conectcar"."cntcar_work"."tb_redshift_ciclodevida_ativacao_stage" at
)
SELECT 
	trunc(intime) as Data
	,Count (a.id) sucessos
	,case when d.anoreferencia is not null then 'Itaú' else C.agregado end as agregado
	,softwareutilizado
	,conveniadoid
	,pista as lane
FROM cntcar_work.tb_bam_dbo_indicedefalhasestacionamento_stage a  
left join info b on a.placa = b.placa and a.intime >= b.data and a.intime < b.datafim
left join cntcar_work.tb_historico_itau_cobranded_stage d on b.adesaoid = d.adesaoid and cast(anoreferencia || '-' || mesreferencia || '-' || '01' as date) = date_trunc('month', a.intime) and checkfaturamento = 1
left join cntcar_work.tb_redshift_ciclodevida_portifolio_stage c on c.adesaoid = b.adesaoid
where a.intime >= '20220101' and a.falha = 0 
group by 1,3,4,5,6