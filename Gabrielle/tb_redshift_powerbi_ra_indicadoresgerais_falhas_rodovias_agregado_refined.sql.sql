/*SERVIDOR: -
AMBIENTE: -
CRIADOR: Matheus Fernandes / Modificado por Gabrielle de Paula
OBJETIVO: Dados referente a índice de falhas de rodovias
PRINCIPAIS INDICADORES: - OBSERVAÇÃO: -
DATA DE ALTERAÇÃO: 24/10/2023
MOTIVO DA ALTERAÇÃO: Migração para procedure e otimização de código.
*/

with base_indicedefalhas as (
    select
        case 
	        when tr.data is not null then tr.data:: date
        	else cast (i.datadepassagem as Date) 
        end as datadepassagem
        ,i.transacaoid
        ,case 
		  when i.statuspassagemid in (1, 9) and i.protocolopassagem = 'edi' then 'falhaedi'
		  when i.ocr = 0 and i.cancelaliberada in (0, 1) and i.passagemautomarica = 0 and i.protocolopassagem = 'mensageria' then 'falhamensageria'
		  when i.protocolopassagem = 'edi' then 'edi'
		  when i.protocolopassagem = 'mensageria' then 'mensageria'
		end as protocolo
        ,case
        	when d.anoreferencia is not null then 'Itaú'
        	else port.agregado 
        end as agregado
        ,conveniadoid
        ,i.praca
    from cntcar_work.tb_redshift_ciclodevida_indicedefalha_stage i
    left join cntcar_work.tb_conectcar_dbo_transacao_stage tr on i.transacaoid = tr.transacaoid
    left join cntcar_work.tb_historico_itau_cobranded_stage d on i.adesaoid = d.adesaoid
    and cast(d.anoreferencia || '-' || d.mesreferencia || '-' || '01' as date) = date_trunc('month', tr.data)
	and checkfaturamento = 1
    left join cntcar_work.tb_redshift_ciclodevida_portifolio_stage port on i.adesaoid = port.adesaoid
    where cast(i.datadepassagem as Date) >= '20210101'
)       
select
    datadepassagem
    ,count(transacaoid) as qtdTransacao
    ,protocolo
    ,agregado
    ,case when conveniadoid in (114, 2375) then 'Sim' else 'Nao' end Dutra
    ,conveniadoid
    ,praca
from base_indicedefalhas A  where datadepassagem >= '20220101'
group by 1,3,4,5,6,7