CREATE OR REPLACE PROCEDURE cntcar_work.spr_trans_rs_esteirasubstituicaoindicesfull_score_carga(OUT salida character varying(100))
 LANGUAGE plpgsql
AS $$
declare
    vmessage varchar(100):='substituicaoScore';
    vcount int;
begin


--- transacoes desconsideradas

/*Premissa #1:
Filtrar motivos desconsiderar que serão carregados das passagens
•	Desconsiderar todas as passagens onde o status do índice esteja computado como 'Desconsiderar'
(Já foram tratados via query nos casos de bloqueio por falta de saldo e não são considerados falhas).
*/

drop table if exists #IndiceDefalhas;
Create table #IndiceDefalhas
(
  id int identity (0,1) ,
  protocolo varchar (100), conveniadoid Varchar (30), ConveniadoPassagemid Varchar (100), DataPassagem timestamp without time zone, codigopraca varchar (30), 
  codigopista varchar (30), obuid Varchar (50),indicefalha varchar (5)
  )
;
insert into #IndiceDefalhas (protocolo, conveniadoid, conveniadopassagemid, datapassagem, codigopraca, codigopista, obuid, indicefalha)
select  *
from  cntcar_work.tb_substituicaotag_indicedefalhas_stage where indicefalha not in (2);


--2.032.455
--Select distinct cast (datapassagem as varchar (10)) from #IndiceDefalhas

--- total de falhas




--- premissa 02  pista com menos de 60% de falhas
/*
Premissa #2:
•	Identificar pistas que geraram um volume alto de falhas
•	Efetuar o carregamento das passagens do dia anterior(D-1).
•	Contabilizar o percentual de falha de cada pista e tirar uma média.
•	Caso a média de falhas daquele dia seja maior ou igual a 60% desconsiderar as passagens daquela pista para análise de TAGs a serem substituídas.
•	O percentual precisa ser parametrizável para conseguirmos acompanhar esses indicadores e irmos alterando caso necessário.
*/

drop table if exists #CalculoEliminacaoPista;
Select  cast(datapassagem as varchar (10) ) as Data, a.conveniadoid, a.codigopraca, a.codigopista, sum (a.indicefalha)Falhas

into #CalculoEliminacaoPista

from #IndiceDefalhas  A
where   a.indicefalha  in (1)
group by a.conveniadoid, a.codigopraca, a.codigopista , cast(datapassagem as varchar (10) );



drop table if exists #Conveniados7dias;
Select conveniadoid, codigopraca, codigopista, sum (falhas)/7 as MediaFalha into #Conveniados7dias
from #CalculoEliminacaoPista  where cast(data as varchar (10) ) >= cast(getdate()-7 as varchar (10) )
group by conveniadoid, codigopraca, codigopista;


drop table if exists #RemoveConveniado;
Select 

a.*, b.mediafalha, (falhas* 100 /mediafalha) PercentualFalha
into #RemoveConveniado

from #CalculoEliminacaoPista a 
left join #Conveniados7dias b on a.conveniadoid= b.conveniadoid and a.codigopraca = b.codigopraca and a.codigopista = b.codigopista
where cast(a.data as varchar (10) ) = cast(getdate()-1 as varchar (10) )
;


insert into cntcar_work.tb_substituicaotag_pistasremovidas_stage
select getdate() as DataProcessamento, * 
from #removeconveniado;



Delete #IndiceDefalhas  
where id  in (select  a.id from #IndiceDefalhas A
  inner join    #RemoveConveniado B On  cast(a.datapassagem  as varchar (10) )  = b.data  and 
a.conveniadoid= b.conveniadoid and a.codigopraca = b.codigopraca and a.codigopista = b.codigopista

where b.PercentualFalha >= 60
)
;


---------------------------------------------------------------------------------



drop table if exists #CalculoScoreSubstituicao;
Select *, 
Case 
when indicefalha = 0 and protocolo = 'Artesp' then '-1'
when indicefalha = 0 and protocolo = 'EDI' then '-1'
when indicefalha = 1 and protocolo = 'Artesp' then '2'
when indicefalha = 1 and protocolo = 'EDI' then '1' 
Else 0 end Score
into #CalculoScoreSubstituicao

from #IndiceDefalhas 
where datapassagem >= cast (getdate() -1 as Varchar (10))

;



Delete cntcar_work.tb_substituicaotag_logscore_stage

where idlogscore in (
Select 
idlogscore
from cntcar_work.tb_substituicaotag_logscore_stage A
inner join #CalculoScoreSubstituicao  B on a.obuid = b.obuid and cast (a.DataOrigemTransacoes  as Varchar (10)) = cast (b.dataPassagem as Varchar (10))
);





insert into cntcar_work.tb_substituicaotag_logscore_stage (datascore, obuid, dataorigemtransacoes, scorereal, score)
Select 
getdate() DataScore, 
obuid, 
cast (dataPassagem as Varchar (10)) as DataOrigemTransacoes,
Sum (Score) scorereal,  
case when Sum (Score) < 0 then 0 else Sum (Score) end  as Score

from #CalculoScoreSubstituicao 
group by obuid,cast (dataPassagem as Varchar (10));




UPDATE
     cntcar_work.tb_substituicaotag_scoreesteirasubstituicao_stage a
SET 
    score = B.score + a.score,
    DataScore = B.DataScore
FROM
   cntcar_work.tb_substituicaotag_logscore_stage AS B

WHERE
	a.obuid = B.obuid  
    AND
    datacancelamento IS NULL 
    AND 
    quarentena = 0
    AND cast(dataorigemtransacoes as varchar (11)) = cast(getdate()-1 as varchar (11))
    ;
    
    
    
 

SELECT INTO salida 'Finished'; 

END;
$$
