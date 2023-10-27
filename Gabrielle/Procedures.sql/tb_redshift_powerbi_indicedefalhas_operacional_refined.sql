CREATE OR REPLACE PROCEDURE cntcar_work.spr_redshift_powerbi_indicedefalhas_operacional()
 LANGUAGE plpgsql
AS $$
	
begin

/*
Criador: Matheus F.
Objetivo: gerar base completa do indice de falhas para alimentar o painel operacional.
Data: 03/01/2023
Alterado por: Gabrielle de Paula
Motivo: Retirar a coluna Placa para melhorar o processamento e diminuir o alto volume de dados.
Data:27/10/2023
*/


drop table if exists cntcar_work.tb_redshift_powerbi_indicedefalhas_operacional_refined;

 DROP TABLE IF EXISTS #indicera;
 select
    case
        when tr.data is not null then tr.data:: date
    else 
    	cast (i.datadepassagem as Date) 
    end as datadepassagem,
    i.adesaoid,
    i.transacaoid,
    i.protocolopassagem as protocolo,
    case
        when i.protocolopassagem = 'edi' then 1
    	else 0 
    	end as PassagemEDI,
    case
        when i.protocolopassagem = 'mensageria' then 1
    	else 0 end as PassagemMensageria,
    case
        when i.statuspassagemid = 1
        and i.protocolopassagem = 'edi' then 1
        else 0 end as FalhaEDI,
    case
        when i.ocr = 0
        and i.cancelaliberada in (0, 1)
        and i.passagemautomarica = 0
        and i.protocolopassagem = 'mensageria' then 1
        else 0 end as FalhaMensageria,
    i.nomefantasia as rodovia,
    i.praca,
    i.pista,
    case
        when i.descricaomodelo = 'TAG Normal' then 'bateria'
        when i.descricaomodelo = 'TAG APP' then 'bateria'
        when i.descricaomodelo = 'CONECTCAR NEW' then 'novo adesivo'
        when i.descricaomodelo = 'CONECTCAR S/NFC' then 'novo adesivo sem nfc'
        else 'adesivo' end as modelotagid,
    c.nomeplano as plano,
    i.classificacao as categoriadoveiculocadastrada,
    --d.placa,
    case
        when cd.anoreferencia is not null then 'Itaú'
        else p.agregado end as agregado
INTO #indicera
from
    cntcar_work.tb_redshift_ciclodevida_indicedefalha_stage i
    left join cntcar_work.tb_conectcar_dbo_transacao_stage tr on i.transacaoid = tr.transacaoid
    left join cntcar_work.tb_redshift_ciclodevida_ativacao_stage c on c.adesaoid = i.adesaoid
    left join cntcar_work.tb_historico_itau_cobranded_stage cd on i.adesaoid = cd.adesaoid
        and cast(cd.anoreferencia || '-' || cd.mesreferencia || '-' || '01' as date) = date_trunc('month', tr.data) and checkfaturamento = 1
    left join cntcar_work.tb_redshift_ciclodevida_portifolio_stage p on i.adesaoid = p.adesaoid
    --left join cntcar_work.tb_conectcar_dbo_veiculo_stage d on d.placa = c.placa
    where
        tr.data >= getdate() - 60;

select
    datadepassagem,
    count(transacaoid) as qtdTransacao,
    sum(PassagemEDI) as qtdPassagemEDI,
    sum(PassagemMensageria) as qtdPassagemMensageria,
    sum(FalhaEDI) as qtdFalhaEDI,
    sum(FalhaMensageria) as qtdFalhaMensageria,
    protocolo,
    agregado,
    i.rodovia,
    i.praca,
    i.pista,
    i.modelotagid,
    i.plano,
    --i.placa,
    i.categoriadoveiculocadastrada
into cntcar_work.tb_redshift_powerbi_indicedefalhas_operacional_refined
from
    #indicera as i
group by
    datadepassagem,
    protocolo,
    agregado,
    i.rodovia,
    i.praca,
    i.pista,
    i.modelotagid,
    i.plano,
    --i.placa,
    i.categoriadoveiculocadastrada;

END;
$$