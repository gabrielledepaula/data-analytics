CREATE OR REPLACE PROCEDURE cntcar_work.spr_redshift_powerbi_ra_indicadoresgerais_falhas_estacionamento_trasits_sucesso()
LANGUAGE plpgsql
AS $$

/*  
Nome de quem criou: Matheus Fernandes Marques
Qual o objetivo ou painel que a proc será usado: Dados referente a indice de falhas de estacionamentos (transits) - sucesso
Data de criação: 
Nome de quem alterou: Gabrielle Silva de Paula
Data de alteração: 17/10/2023
Motivo da alteração: Migração para procedure e otimização de código.
*/
 

begin

drop table if exists cntcar_work.tb_redshift_powerbi_ra_indicadoresgerais_falhas_estacionamento_trasits_sucesso_refined; -- adicionar e alterar

drop table if exists #info;
SELECT 
    at.data 
    ,at.adesaoid
    ,at.placa
    ,case when at.datacancelamento is not null then at.datacancelamento 
    else getdate() end as datafim
into #info
FROM "conectcar"."cntcar_work"."tb_redshift_ciclodevida_ativacao_stage" at;

drop table if exists #base;
    SELECT 
        trunc(intime) as Data
        ,Count (a.id) sucessos
        ,case when d.anoreferencia is not null then 'Itaú' else C.agregado end as agregado
        ,softwareutilizado
        ,conveniadoid
        ,pista as lane
    into #base
    FROM cntcar_work.tb_bam_dbo_indicedefalhasestacionamento_stage a  
    left join info b on a.placa = b.placa and a.intime >= b.data and a.intime < b.datafim
    left join cntcar_work.tb_historico_itau_cobranded_stage d on b.adesaoid = d.adesaoid and cast(anoreferencia || '-' || mesreferencia || '-' || '01' as date) = date_trunc('month', a.intime) and checkfaturamento = 1
    left join cntcar_work.tb_redshift_ciclodevida_portifolio_stage c on c.adesaoid = b.adesaoid
    where a.intime >= '20220101' and a.falha = 0 
    group by 1,3,4,5,6;

select * 
into cntcar_work.tb_redshift_powerbi_ra_indicadoresgerais_falhas_estacionamento_trasits_sucesso_refined
from base

END;
$$