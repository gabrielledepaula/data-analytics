CREATE OR REPLACE PROCEDURE cntcar_work.spr_trans_rs_esteirasubstituicaoindicesfull_carga(OUT salida character varying(100))
 LANGUAGE plpgsql
AS $$
declare
    vmessage varchar(100):='IndicedefalhasEsteiraSubsFull';
    vcount int;
begin
      
--truncate table cntcar_work.tb_substituicaotag_indicedefalhasFull_stage;

insert
	into
	cntcar_work.tb_substituicaotag_indicedefalhasFull_stage
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
		p.datatransacao as DataTransacao
from
	cntcar_work.tb_substituicaotag_indicedefalhas_artesp_stage p
	;

insert
	into
	cntcar_work.tb_substituicaotag_indicedefalhasFull_stage

select 
	'EDI' as Protocolo,
	atrn.ConveniadoId as ConveniadoId, 
	dtrn.DetalheTRNId as ConveniadoPassagemId,
	dtrn.Data as DataPassagem,
	dtrn.NumeroPraca as CodigoPraca, 
	dtrn.NumeroPista as CodigoPista, 
	dtrn.NumeroTag as Obuid,
	case when (dtrn.StatusPassagemId = 1) then 1 else 0 end as IndiceFalha,
	t.data as DataTransacao
from 
	cntcar_work.tb_ConectCar_dbo_DetalheTRN_stage dtrn
	inner join cntcar_work.tb_conectcar_dbo_ArquivoTRN_stage atrn on atrn.ArquivoTRNId = dtrn.ArquivoTRNId
	left join cntcar_work.tb_conectcar_dbo_transacaopassagem_stage tp on dtrn.detalhetrnid = tp.detalhetrnid
	LEFT JOIN cntcar_work.tb_conectcar_dbo_transacao_stage t on tp.transacaoid = t.transacaoid
where
	dtrn.StatusCobrancaId <> 2 and 
	datatransacao >= dateadd(day, -1,date_trunc('day',getdate())) and datatransacao < date_trunc('day',getdate());
    
    
SELECT INTO salida 'Finished'; 

END;
$$