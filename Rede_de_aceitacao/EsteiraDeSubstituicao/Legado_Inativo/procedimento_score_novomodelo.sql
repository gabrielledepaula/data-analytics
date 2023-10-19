drop table if exists #baseindice;
SELECT 
    'Artesp' AS Protocolo,
    p.ConveniadoPassagemId AS ConveniadoPassagemId,
	p.NumeroReenvio,
	m.ConcessionariaId,
    DATEADD(S, p.DataHora, '1970-01-01') AS DataPassagem,
    p.Praca AS CodigoPraca, 
    p.Pista AS CodigoPista, 
    p.TagId AS Obuid,
    (
        CASE            
			 WHEN (
                    p.PassagemAutomatica = 1 OR 
                    p.MotivoManualId = 6 OR  -- DeteccaoPorOcr
					p.TagLida = 1 OR 
					p.Ocr = 1
                ) THEN 0 -- Automatica
			WHEN (
                    p.PassagemAutomatica = 0 AND 
                    p.MotivoManualId = 2 AND  -- FalhaLeituraTag
                    p.TagLida = 0 AND 
                    p.Ocr = 0
                  ) 
					THEN 1 -- FalhaLeituraTag
					ELSE 2 -- Desconsiderar
				 END
    ) AS IndiceFalha,
	m.DataCriacao, 
    p.MensagemItemId,
    t.data as datatransacao,
    t.adesaoid
into #baseindice
FROM 
    cntcar_work.tb_ConectCarMensageria_dbo_Passagem_stage p 
    INNER JOIN cntcar_work.tb_ConectCarMensageria_dbo_MensagemItem_stage mi  ON p.MensagemItemId = mi.Id
    INNER JOIN cntcar_work.tb_ConectCarMensageria_dbo_Mensagem_stage m  ON mi.MensagemId = m.Id
    LEFT JOIN cntcar_work.tb_conectcar_dbo_passagem_stage p1  on p.mensagemitemid = p1.mensagemitemid
    LEFT JOIN cntcar_work.tb_conectcar_dbo_transacaopassagem_stage tp  on p1.passagemid = tp.passagemid
    LEFT JOIN cntcar_work.tb_conectcar_dbo_transacao_stage t on tp.transacaoid = t.transacaoid
    where numeroreenvio = 0 and datatransacao >= dateadd(day, -30,date_trunc('day',getdate())) and datatransacao < date_trunc('day',getdate())
;

drop table if exists #tb_substituicaotag_indicedefalhas_artesp_stage;
select * into #tb_substituicaotag_indicedefalhas_artesp_stage from #baseindice where IndiceFalha = 1;

drop table if exists #tb_substituicaotag_indicedefalhasFull_stage;
select 
		p.Protocolo as Protocolo,
		(
			select
				Conveniado_Id
			from
				cntcar_work.tb_Conectcar_dbo_Conveniado_stage
			where
				Codigo_Protocolo_Artesp = p.ConcessionariaId
		) as ConveniadoId,
		p.ConveniadoPassagemId as ConveniadoPassagemId,
		p.DataPassagem as DataPassagem,
		p.CodigoPraca as CodigoPraca, 
		p.CodigoPista as CodigoPista, 
		p.Obuid as Obuid,
		p.IndiceFalha as IndiceFalha,
		date(p.datatransacao) as DataTransacao,
		p.adesaoid
		into #tb_substituicaotag_indicedefalhasFull_stage
from
	#tb_substituicaotag_indicedefalhas_artesp_stage p
union
select 
	'EDI' as Protocolo,
	atrn.ConveniadoId as ConveniadoId, 
	dtrn.DetalheTRNId as ConveniadoPassagemId,
	dtrn.Data as DataPassagem,
	dtrn.NumeroPraca as CodigoPraca, 
	dtrn.NumeroPista as CodigoPista, 
	dtrn.NumeroTag as Obuid,
	case when (dtrn.StatusPassagemId = 1) then 1 else 0 end as IndiceFalha,
	date(t.data) as DataTransacao,
	t.adesaoid
from 
	cntcar_work.tb_ConectCar_dbo_DetalheTRN_stage dtrn
	inner join cntcar_work.tb_conectcar_dbo_ArquivoTRN_stage atrn on atrn.ArquivoTRNId = dtrn.ArquivoTRNId
	left join cntcar_work.tb_conectcar_dbo_transacaopassagem_stage tp on dtrn.detalhetrnid = tp.detalhetrnid
	LEFT JOIN cntcar_work.tb_conectcar_dbo_transacao_stage t on tp.transacaoid = t.transacaoid
where
	dtrn.StatusCobrancaId <> 2 and 
	datatransacao >= dateadd(day, -30,date_trunc('day',getdate())) and datatransacao < date_trunc('day',getdate());

-- teste 
--select * from #tb_substituicaotag_indicedefalhasFull_stage limit 100
---------------------------------------------------------------------------------------------------------

drop table if exists #IndiceDefalhas;
Create table #IndiceDefalhas
(
  id int identity (0,1) ,
  protocolo varchar (100), conveniadoid Varchar (30), ConveniadoPassagemid Varchar (100), DataPassagem timestamp without time zone, DataTransacao timestamp without time zone, codigopraca varchar (30), 
  codigopista varchar (30), obuid Varchar (50),indicefalha varchar (5), adesaoid int, agregado varchar (20)
  )
;
insert into #IndiceDefalhas (protocolo, conveniadoid, conveniadopassagemid, datapassagem, DataTransacao, codigopraca, codigopista, obuid, indicefalha, adesaoid, agregado)
select  protocolo, conveniadoid, conveniadopassagemid, datapassagem, DataTransacao, codigopraca, codigopista, obuid, indicefalha, a.adesaoid, case when d.anoreferencia is not null then 'Itaú' else C.agregado end as agregado
from  #tb_substituicaotag_indicedefalhasFull_stage a
left join cntcar_work.tb_redshift_ciclodevida_portifolio_stage c on c.adesaoid = a.adesaoid
left join cntcar_work.tb_historico_itau_cobranded_stage d on a.adesaoid = d.adesaoid and cast(anoreferencia || '-' || mesreferencia || '-' || '01' as date) = date_trunc('month', getdate()) and checkfaturamento = 1
where indicefalha not in (2);

---------------------------------------------------------------------------------------------------------
--Criação de tabelas parametros, ela seria o CSV transformado em tabela com o uso do crawler
drop table if exists #Parametros;
Create table #Parametros
(
  pontuacaofalhaartesp smallint, pontuacaosucessoartesp smallint, pontuacaofalhaedi smallint, pontuacaosucessoedi smallint,
  qtdtagsliberadas smallint, agregado varchar(20), scoreminimoparatroca smallint
  )
;
insert into #Parametros (pontuacaofalhaartesp, pontuacaosucessoartesp, pontuacaofalhaedi, pontuacaosucessoedi,
  qtdtagsliberadas, agregado, scoreminimoparatroca)
select  2 as pontuacaofalhaartesp, -1 as pontuacaosucessoartesp, 1 as pontuacaofalhaedi, -1 as pontuacaosucessoedi,
  1000 qtdtagsliberadas, 'Completo' as agregado, 1 as scoreminimoparatroca;


--select * from #Parametros;




---------------------------------------------------------------------------------------------------------

drop table if exists #CalculoScoreSubstituicao;
Select *, 
Case 
when indicefalha = 0 and protocolo = 'Artesp' then (select pontuacaosucessoartesp from #Parametros p where upper(i.agregado) = upper(p.agregado))
when indicefalha = 0 and protocolo = 'EDI' then (select pontuacaosucessoedi from #Parametros p where upper(i.agregado) = upper(p.agregado))
when indicefalha = 1 and protocolo = 'Artesp' then (select pontuacaofalhaartesp from #Parametros p where upper(i.agregado) = upper(p.agregado))
when indicefalha = 1 and protocolo = 'EDI' then (select pontuacaosucessoedi from #Parametros p where upper(i.agregado) = upper(p.agregado)) 
Else 0 end Score
into #CalculoScoreSubstituicao

from #IndiceDefalhas i
where datapassagem >= cast (getdate() -1 as Varchar (10))

;



--Delete cntcar_work.tb_substituicaotag_logscore_stage

--where idlogscore in (
--Select 
--idlogscore
--from cntcar_work.tb_substituicaotag_logscore_stage A
--inner join #CalculoScoreSubstituicao  B on a.obuid = b.obuid and cast (a.DataOrigemTransacoes  as Varchar (10)) = cast (b.dataPassagem as Varchar (10))
--);





drop table if exists #tb_substituicaotag_logscore_stage;
Select 
getdate() DataScore, 
obuid, 
dataPassagem  as DataOrigemTransacoes,
Sum (Score) scorereal,  
case when Sum (Score) < 0 then 0 else Sum (Score) end  as Score
into #tb_substituicaotag_logscore_stage
from #CalculoScoreSubstituicao 
group by obuid,dataPassagem ;

--insere score zerados para começar o indice do zero
-- ATENÇÃO: ESSES VALORES DEVEM SER INSERIDOS EM UMA TABELA E APÓS ISSO DESCOMENTAR A PARTE DO AND, SE MANTER ELE IRA ZERAR O INDICE NOVAMENTE
drop table if exists #tb_substituicaotag_scoreesteirasubstituicao_stage;
select a.clienteid, a.adesaoid, a.tagid, a.datacancelamento, a.obuid, 0::int as score, '20000101'::date as datascore, 0 as quarentena, case when d.anoreferencia is not null then 'Itaú' else C.agregado end as agregado
into #tb_substituicaotag_scoreesteirasubstituicao_stage 
from cntcar_work.tb_redshift_ciclodevida_ativacao_stage a
left join cntcar_work.tb_redshift_ciclodevida_portifolio_stage c on c.adesaoid = a.adesaoid
left join cntcar_work.tb_historico_itau_cobranded_stage d on a.adesaoid = d.adesaoid and cast(anoreferencia || '-' || mesreferencia || '-' || '01' as date) = date_trunc('month', getdate()) and checkfaturamento = 1
where a.datacancelamento is null -- and adesaoid not in (select adesaoid from tb_substituicaotag_scoreesteirasubstituicao_novo_stage);
;
----------------------------------------------------------------------------
--Atualiza os scores com os scores d-1

UPDATE
     #tb_substituicaotag_scoreesteirasubstituicao_stage a
SET 
    score = B.score + a.score,
    DataScore = B.DataScore
FROM
   #tb_substituicaotag_logscore_stage AS B

WHERE
	a.obuid = B.obuid  
    AND
    datacancelamento IS NULL 
    AND 
    quarentena = 0
    --AND cast(dataorigemtransacoes as varchar (11)) = cast(getdate()-1 as varchar (11));
    ;
--testes
   --select * from #tb_substituicaotag_scoreesteirasubstituicao_stage where score > 0 order by score desc;

   --select * from #tb_substituicaotag_scoreesteirasubstituicao_stage a where score > 0 order by score desc limit 100

----------------------------------------------------------------------------
-- gera o lote de tags - como será feito com unload essa parte do código deve ser separado para outra procedure, executada 1 vez ao mês ou quando solicitada   
drop table if exists #scoreexportar;
select score.clienteid, score.adesaoid, score.tagid, score.obuid, case when score.score is null then 0 else score.score end as score, score.datascore, 
date_trunc('day',getdate()) as datalote, 
'teste' as lote, --alterar para case when (select max(lote) from cntcar_work.tb_substituicaotag_lote_stage) is not null then (select max(lote) from cntcar_work.tb_substituicaotag_lote_stage)+1 else 1 end as lote
case when d.anoreferencia is not null then 'Itaú' else C.agregado end as agregado,
a.datacancelamento
into #scoreexportar 
from #tb_substituicaotag_scoreesteirasubstituicao_stage score
left join cntcar_work.tb_redshift_ciclodevida_ativacao_stage a on score.adesaoid = a.adesaoid
left join cntcar_work.tb_redshift_ciclodevida_portifolio_stage c on c.adesaoid = a.adesaoid
left join cntcar_work.tb_historico_itau_cobranded_stage d on a.adesaoid = d.adesaoid and cast(anoreferencia || '-' || mesreferencia || '-' || '01' as date) = date_trunc('month', getdate()) and checkfaturamento = 1
;


drop table if exists #basefinalscores;
select *, 
ROW_NUMBER() OVER (
PARTITION BY agregado
ORDER BY score DESC
)  as rn
into #basefinalscores
from #scoreexportar;

--teste
--select * from #basefinalscores where agregado='Completo' limit 1000
drop table if exists #tempPDDBase;
select * into #tempPDDBase
from #basefinalscores sc 
where upper(agregado) in (select upper(agregado) from #Parametros) 
and rn <= (select max(qtdtagsliberadas) from #Parametros p where upper(p.agregado) = upper(sc.agregado))
and score >= (select max(scoreminimoparatroca) from #Parametros p where upper(p.agregado) = upper(sc.agregado))
;

select * from #tempPDDBase

UNLOAD ('select * from #tempPDDBase') 
TO 's3://cntcar-dlk-dev-us-east-2-564512845791-data-analytics/Financeiro_PDD/unload_redshift/esteiradesubstituicao_novofluxo_d-30.csv'
IAM_ROLE 'arn:aws:iam::564512845791:role/sdlf-engineering-AWSRedshiftRole'
CSV
HEADER   
PARALLEL OFF ;