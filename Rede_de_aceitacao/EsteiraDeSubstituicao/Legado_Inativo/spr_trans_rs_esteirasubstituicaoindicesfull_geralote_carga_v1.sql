CREATE OR REPLACE PROCEDURE cntcar_work.spr_trans_rs_esteirasubstituicaoindicesfull_geralote_carga(OUT salida character varying(100))
 LANGUAGE plpgsql
AS $$
declare
    vmessage varchar(100):='esteirasubscontrolecliente';
    vcount int;
begin

insert into cntcar_work.tb_substituicaotag_lote_stage
Select clienteid, Adesaoid, a.tagid, obuid, Score, date_trunc('day',getdate()) as Datalote
from cntcar_work.tb_substituicaotag_scoreesteirasubstituicao_stage A
where score > (select score from tabelaparametro) and planoid in (select planoid from tabelaparametro) order by score, clienteid desc
limit (select qtdtagsliberadasparatrocamensal from tabelaparametro);

SELECT INTO salida 'Finished'; 

END;
$$
