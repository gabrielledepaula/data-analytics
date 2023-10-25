/*SERVIDOR: -
AMBIENTE: -
CRIADOR: Matheus Fernandes / Modificado por Gabrielle de Paula
OBJETIVO: Dados referente a indice de falhas de rodovias - Logística
PRINCIPAIS INDICADORES: - OBSERVAÇÃO: -
DATA DE ALTERAÇÃO: 24/10/2023
MOTIVO DA ALTERAÇÃO: Migração para procedure e otimização de código.
*/

with base as (
	select
	        case when tr.data is not null then tr.data:: date else cast (i.datadepassagem as Date) end as datadepassagem
	        ,i.adesaoid
	        ,i.transacaoid
	        ,i.protocolopassagem as protocolo_passagem
	        ,case 
			  when i.statuspassagemid in (1, 9) and i.protocolopassagem = 'edi' then 'falhaedi'
			  when i.ocr = 0 and i.cancelaliberada in (0, 1) and i.passagemautomarica = 0 and i.protocolopassagem = 'mensageria' then 'falhamensageria'
			  when i.protocolopassagem = 'edi' then 'edi'
			  when i.protocolopassagem = 'mensageria' then 'mensageria'
			end as protocolo
	        ,i.nomefantasia as rodovia
	        ,i.praca
	        ,i.pista
	        ,case
	        when i.descricaomodelo = 'TAG Normal' then 'bateria'
	        when i.descricaomodelo = 'TAG APP' then 'bateria'
	        when i.descricaomodelo = 'CONECTCAR NEW' then 'novo adesivo'
	        when i.descricaomodelo = 'CONECTCAR S/NFC' then 'novo adesivo sem nfc'
	        else 'adesivo' end as modelotagid
	        ,i.classificacao as categoriadoveiculocadastrada
	        ,case when cd.anoreferencia is not null then 'Itaú' else p.agregado end as agregado
			,bt.datafabricacao
			,bt.lotefabricacao
			,bt.fabricante
			,bt.descricao_modelo
	from cntcar_work.tb_redshift_ciclodevida_indicedefalha_stage i 
	inner join cntcar_work.tb_conectcar_dbo_transacao_stage tr on i.transacaoid = tr.transacaoid
	left join cntcar_work.tb_historico_itau_cobranded_stage cd on i.adesaoid = cd.adesaoid
	and cast(cd.anoreferencia || '-' || cd.mesreferencia || '-' || '01' as date) = date_trunc('month', tr.data) and checkfaturamento = 1
	inner join cntcar_work.tb_redshift_ciclodevida_portifolio_stage p on i.adesaoid = p.adesaoid
	left join (
		SELECT distinct 
			t.obuid
			,t.datafabricacao 
			,t.lotefabricacao
			,f.identificador as fabricante
			,mt.descricao_modelo 
		FROM "conectcar"."cntcar_work"."tb_conectcar_dbo_tag_stage" t 
		JOIN "conectcar"."cntcar_work"."tb_conectcar_dbo_kitdetag_stage" k ON k.Kit_De_Tag_Id = t.KitId
		JOIN "conectcar"."cntcar_work"."tb_conectcar_dbo_remessadetags_stage" r ON r.Remessa_De_Tags_Id = k.remessa_de_tags_id
		JOIN "conectcar"."cntcar_work"."tb_conectcar_dbo_fabricante_stage" f ON f.FabricanteId = r.Fabricante_Id
		JOIN "conectcar"."cntcar_work"."tb_conectcar_dbo_modelotag_stage" mt on t.modelotagid = mt.modelo_tag_id
		where t.deletado is false) bt on bt.obuid=i.obuid
	where case when tr.data is not null then tr.data:: date
	else cast (i.datadepassagem as Date) end  >= dateadd('day',-60,date_trunc('day',getdate()))
  ) 
  
select
	datadepassagem
	,count(transacaoid) as qtdTransacao
    ,protocolo_passagem
    ,protocolo
    ,agregado
    ,rodovia
    ,praca
    ,pista
    ,modelotagid
	,datafabricacao
	,lotefabricacao
	,fabricante
	,descricao_modelo 
from base
group by 1,3,4,5,6,7,8,9,10,11,12,13
