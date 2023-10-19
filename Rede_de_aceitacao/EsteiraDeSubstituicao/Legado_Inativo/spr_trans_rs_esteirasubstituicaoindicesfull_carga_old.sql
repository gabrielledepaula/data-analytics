CREATE OR REPLACE PROCEDURE cntcar_work.spr_trans_rs_esteirasubstituicaoindicesfull_carga(OUT salida character varying(100))
 LANGUAGE plpgsql
AS $$
declare
    vmessage varchar(100):='IndicedefalhasEsteiraSubsFull';
    vcount int;
begin
      
truncate table cntcar_work.tb_substituicaotag_indicedefalhasFull_stage;

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
		Codigo_Protocolo_Artesp = p.ConcessionariaId) as ConveniadoId,
		p.ConveniadoPassagemId as ConveniadoPassagemId,
		p.DataPassagem as DataPassagem,
		p.CodigoPraca as CodigoPraca, 
		p.CodigoPista as CodigoPista, 
		p.Obuid as Obuid,
		p.IndiceFalha as IndiceFalha
from
	cntcar_work.tb_substituicaotag_indicedefalhas_artesp_stage p
where
	p.DataPassagem >= Getdate ()-60
	and 
    p.Numeroenvio = 
						(
	select
		max(p2.NumeroReenvio)
	from
		cntcar_work.tb_ConectCarMensageria_dbo_passagem_stage p2
	inner join cntcar_work.tb_ConectCarMensageria_dbo_MensagemItem_stage mi2 on
		mi2.Id = p2.MensagemItemId
	inner join cntcar_work.tb_ConectCarMensageria_dbo_Mensagem_stage m2 on
		m2.Id = mi2.MensagemId
	where 
										p2.ConveniadoPassagemId = p.ConveniadoPassagemId
		and m2.ConcessionariaId = p.ConcessionariaId
						)
	-- obter sempre o �ltimo reenvio

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
		(
			case 
				when (
						dtrn.StatusPassagemId = 1
		-- Manual
					) then 1
		else 0
	end
		) as IndiceFalha
from 
		cntcar_work.tb_ConectCar_dbo_DetalheTRN_stage dtrn
inner join cntcar_work.tb_conectcar_dbo_ArquivoTRN_stage atrn on
	atrn.ArquivoTRNId = dtrn.ArquivoTRNId
where
	dtrn.StatusCobrancaId <> 2
	and DataPassagem >= Getdate ()-60;
    
    
--SELECT INTO salida 'Finished'; 

END;
$$
