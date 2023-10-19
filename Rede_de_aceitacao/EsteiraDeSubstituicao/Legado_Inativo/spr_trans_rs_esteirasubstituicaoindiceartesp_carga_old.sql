CREATE OR REPLACE PROCEDURE cntcar_work.spr_trans_rs_esteirasubstituicaoindiceartesp_carga(OUT salida character varying(100))
 LANGUAGE plpgsql
AS $$
declare
    vmessage varchar(100):='IndicedefalhasEsteiraSubs';
    vcount int;
begin
  
  
  truncate table cntcar_work.tb_substituicaotag_indicedefalhas_artesp_stage;

insert into cntcar_work.tb_substituicaotag_indicedefalhas_artesp_stage
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
    p.MensagemItemId
    
       
 

FROM 
    cntcar_work.tb_ConectCarMensageria_dbo_Passagem_stage p 
    INNER JOIN cntcar_work.tb_ConectCarMensageria_dbo_MensagemItem_stage mi  ON p.MensagemItemId = mi.Id
    INNER JOIN cntcar_work.tb_ConectCarMensageria_dbo_Mensagem_stage m  ON mi.MensagemId = m.Id


;

SELECT INTO salida 'Finished'; 

END;
$$