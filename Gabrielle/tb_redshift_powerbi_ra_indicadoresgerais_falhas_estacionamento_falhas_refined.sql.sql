/*SERVIDOR: -
AMBIENTE: -
CRIADOR: Matheus Fernandes / Modificado por Gabrielle de Paula
OBJETIVO: Dados referente a indice de falhas de estacionamentos - falhas
PRINCIPAIS INDICADORES: - OBSERVAÇÃO: -
DATA DE ALTERAÇÃO: 24/10/2023
MOTIVO DA ALTERAÇÃO: Migração para procedure e otimização de código.
*/

-- Informações Sobre Adesão de TAG
with adesaotag as(
	Select
	    a.adesaoid
	    ,a.clienteid
	    ,tg.tagid
	    ,tg.datafabricacao
	    ,mt.descricao_modelo as ModeloTag
	    ,cv.classificacao
	    ,cv.rodagem
	    ,v.marca
	    ,obuid
	    ,v.placa
	from cntcar_work.tb_conectcar_dbo_adesao_stage A
	INNER JOIN cntcar_work.tb_conectcar_dbo_tag_stage tg ON a.TagId = tg.TagId
	INNER JOIN cntcar_work.tb_conectcar_dbo_modelotag_stage mt ON tg.ModeloTagId = mt.Modelo_Tag_Id
	INNER JOIN cntcar_work.tb_conectcar_dbo_veiculo_stage v ON a.VeiculoId = v.Veiculo_Id
	LEFT JOIN cntcar_work.tb_conectcar_dbo_categoriaveiculo_stage cv ON cv.Categoria_Veiculo_Id = v.Categoria_Id
)

,base as(
	SELECT
	    count(t.transacaoid) as transacao_qtd,
	    0 as transacao_qtd_sucesso,
	    DATE_TRUNC('day', te.datahoratransacao) as data_passagem,
	    DATE_TRUNC('day', t.data) as data_processamento,
	    case when d.anoreferencia is not null then 'Itaú' else ptf.agregado end as agregado
	FROM
    cntcar_work.tb_conectcar_dbo_transacaoestacionamento_stage te
    INNER JOIN cntcar_work.tb_conectcar_dbo_transacao_stage t ON te.transacaoid = t.transacaoid
    INNER JOIN adesaotag a ON t.AdesaoId = a.AdesaoId
    LEFT JOIN cntcar_work.tb_conectcar_dbo_praca_stage p ON p.Praca_Id = te.PracaId --
    LEFT JOIN cntcar_work.tb_conectcar_dbo_pista_stage pis ON pis.pista_id = te.pistaid --
    INNER JOIN cntcar_work.tb_conectcar_dbo_conveniado_stage c ON c.conveniado_id = te.conveniadoid
    LEFT JOIN cntcar_work.tb_conectcar_dbo_softwareutilizado_stage su ON su.software_utilizado_id = c.software_utilizado_id --
    INNER JOIN cntcar_work.tb_conectcar_dbo_parceironegocio_stage pn ON pn.parceironegocioid = c.conveniado_id --
    LEFT JOIN cntcar_work.tb_conectcar_dbo_grupoparceironegocio_stage gp ON gp.Grupo_Parceiro_Negocio_Id = pn.GrupoParceiroNegocioId --
    left join cntcar_work.tb_historico_itau_cobranded_stage d on a.adesaoid = d.adesaoid and cast(anoreferencia || '-' || mesreferencia || '-' || '01' as date) = date_trunc('month', t.data) and checkfaturamento = 1
	left join cntcar_work.tb_redshift_ciclodevida_portifolio_stage ptf on ptf.adesaoid = a.adesaoid
	WHERE
    c.pagamento_por_placa = 0
    AND te.tipotransacaoestacionamentoid = 1 
	group by 
    data_passagem,
    data_processamento,
    case when d.anoreferencia is not null then 'Itaú' else ptf.agregado end
)    

,base2 as(
	SELECT
	    0 as transacao_qtd_sucesso,
	    count(t.transacaoid) as transacao_qtd_sucesso,
	    DATE_TRUNC('day', te.datahoratransacao) as data_passagem,
	    DATE_TRUNC('day', t.data) as data_processamento,
	    case when d.anoreferencia is not null then 'Itaú' else ptf.agregado end as agregado 
	FROM
    	cntcar_work.tb_conectcar_dbo_transacaoestacionamento_stage te
    INNER JOIN cntcar_work.tb_conectcar_dbo_transacao_stage t ON te.transacaoid = t.transacaoid
    INNER JOIN adesaotag a ON t.AdesaoId = a.AdesaoId
    LEFT JOIN cntcar_work.tb_conectcar_dbo_praca_stage p ON p.Praca_Id = te.PracaId
    LEFT JOIN cntcar_work.tb_conectcar_dbo_pista_stage pis ON pis.pista_id = te.pistaid
    INNER JOIN cntcar_work.tb_conectcar_dbo_conveniado_stage c ON c.conveniado_id = te.conveniadoid
    LEFT JOIN cntcar_work.tb_conectcar_dbo_softwareutilizado_stage su ON su.software_utilizado_id = c.software_utilizado_id
    INNER JOIN cntcar_work.tb_conectcar_dbo_parceironegocio_stage pn ON pn.parceironegocioid = c.conveniado_id
    LEFT JOIN cntcar_work.tb_conectcar_dbo_grupoparceironegocio_stage gp ON gp.Grupo_Parceiro_Negocio_Id = pn.GrupoParceiroNegocioId
    left join cntcar_work.tb_historico_itau_cobranded_stage d on a.adesaoid = d.adesaoid and cast(anoreferencia || '-' || mesreferencia || '-' || '01' as date) = date_trunc('month', t.data) and checkfaturamento = 1
	left join cntcar_work.tb_redshift_ciclodevida_portifolio_stage ptf on ptf.adesaoid = a.adesaoid
	WHERE
    	c.pagamento_por_placa = 0
    	AND te.tipotransacaoestacionamentoid = 0 
	group by 
	    data_passagem,
	    data_processamento,
	    case when d.anoreferencia is not null then 'Itaú' else ptf.agregado end
)
select * from base 
union 
select * from base2
