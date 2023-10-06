    ------------------------------------
    -------- DADOS CADASTRAIS ----------
    -----------  RODOVIAS  -------------
    ------------------------------------
    /*
    SERVIDOR: cntcar_work
    AMBIENTE: redshift
    CRIADOR: Raul
    OBJETIVO: Principais informações cadastrais de rodovias pedagiadas, a conectcar em 2023 está presente em 100% delas
    PRINCIPAIS INDICADORES: Razaão Social da Rodovia, Informações Praça, Informações Pista, Endereço da Praça
    OBSERVAÇÃO: Relação Rodovia (1) x Praça (n) x Pista (n) -> 1 rodovia possui 1 ou mais praça e uma praça possui 1 ou mais pistas.
    "Pista são as cabines com as cancelas, o dado mais granular da Rodovia"
    DATA CRIAÇÃO: 23/01/2023
    */
    
    drop table if exists #tabelaCadastralPista;
    SELECT  DISTINCT
        f.razaosocial,
        h.nome as grupo,
        a.pista_id,
        a.praca_id,
        e.SIGLA as UF
    
    into #tabelaCadastralPista
    
        from "conectcar"."cntcar_work"."tb_conectcar_dbo_pista_stage" AS a
        left join "conectcar"."cntcar_work"."tb_conectcar_dbo_praca_stage" AS b
        on a.praca_id = b.praca_id
        LEFT JOIN"conectcar"."cntcar_work"."tb_conectcar_dbo_conveniado_stage" AS c
        on b.conveniado_id = c.conveniado_id
        LEFT JOIN "conectcar"."cntcar_work"."tb_conectcar_dbo_cidade_stage" as d
        on B.cidade_id = d.cidade_id  
        LEFT JOIN "conectcar"."cntcar_work"."tb_conectcar_dbo_estado_stage" as e
        ON B.estado_id = e.estado_id
        left join"conectcar"."cntcar_work"."tb_conectcar_dbo_parceironegocio_stage" as f
        on b.conveniado_id = f.parceironegocioid
        left join "cntcar_work"."tb_conectcar_dbo_tipoparceironegocio_stage" as g
        on g.id = f.tipoparceironegocioid
        left join "conectcar"."cntcar_work"."tb_conectcar_dbo_grupoparceironegocio_stage"as h
        on  f.grupoparceironegocioid = h.grupo_parceiro_negocio_id
        
        where g.nome in ('Concessionaria')
            
        order by 
        1,2,3
        asc
    ;
    
    select 
        a.razaosocial,
        case when a.grupo is null
        then 'nao encontrado'
        else a.grupo
        end grupo,
        replace(left(b.datadepassagem::varchar, 4) || right(left(b.datadepassagem::varchar, 7),2), '-','') anoMes,
        count (b.transacaoid) as qtdtransacoes,
        a.UF
    from #tabelaCadastralPista as a
    left join cntcar_work.tb_conectcar_dbo_transacaopassagem_stage as b 
        on a.pista_id = b.pistaid 
    where cast(b."datadepassagem" as date) >= '20230401'
    group by 3,1,2,5