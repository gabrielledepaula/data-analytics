CREATE OR REPLACE PROCEDURE cntcar_work.spr_redshift_powerbi_ra_indicadoresgerais_falhas_rodovias_agregado()
 LANGUAGE plpgsql
AS $$

/*  
Nome de quem criou: Matheus Fernandes Marques
Qual o objetivo ou painel que a proc será usado: Dados referente a indice de falhas de rodovias - Seleção distinta da coluna agregado
Data de criação: 
Nome de quem alterou: Gabrielle Silva de Paula
Data de alteração: 24/10/2023
Motivo da alteração: Migração para procedure e otimização de código.
*/
 

begin

drop table if exists cntcar_work.tb_redshift_powerbi_ra_indicadoresgerais_falhas_rodovias_agregado_refined; 

drop table if exists #base_indicedefalhas;
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
    INTO #base_indicedefalha;
    from cntcar_work.tb_redshift_ciclodevida_indicedefalha_stage i
    left join cntcar_work.tb_conectcar_dbo_transacao_stage tr on i.transacaoid = tr.transacaoid
    left join cntcar_work.tb_historico_itau_cobranded_stage d on i.adesaoid = d.adesaoid
    and cast(d.anoreferencia || '-' || d.mesreferencia || '-' || '01' as date) = date_trunc('month', tr.data)
    and checkfaturamento = 1
    left join cntcar_work.tb_redshift_ciclodevida_portifolio_stage port on i.adesaoid = port.adesaoid
    where cast(i.datadepassagem as Date) >= '20210101';
   
select
    datadepassagem
    ,count(transacaoid) as qtdTransacao
    ,protocolo
    ,agregado
    ,case when conveniadoid in (114, 2375) then 'Sim' else 'Nao' end Dutra
    ,conveniadoid
    ,praca
INTO cntcar_work.tb_redshift_powerbi_ra_indicadoresgerais_falhas_rodovias_agregado_refined
from base_indicedefalhas A  where datadepassagem >= '20220101'
group by 1,3,4,5,6,7

END;
$$