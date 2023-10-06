--atalhos shift + alt + f deixa bonito

/*
---tratando a primeira tabela 
 drop TABLE IF EXISTS #tb_primeirahigienizacaopf;

CREATE TABLE #tb_primeirahigienizacaopf( cpf int8, nome varchar(500), datanascimento varchar(500), nomemae varchar(500), cep int8, logradouro varchar(500), numero varchar(500), complemento varchar(500), cidade varchar(500), bairro varchar(500), statusdareceitaonline varchar(500), profissao varchar(500), renda varchar(500), cpf_padr varchar(500), nome_padr varchar(500), nomemae_padr varchar(500), nasc_padr varchar(500), logr_tipo1_padr varchar(500), logr_titulo1_padr varchar(500), logr_nome1_padr varchar(500), logr_numero1_padr varchar(500), logr_complemento1_padr varchar(500), endereco1_padr varchar(500), bairro1_padr varchar(500), cidade1_padr varchar(500), uf1_padr varchar(500), cep1_padr varchar(500), nome_confirmado varchar(500), data_nascimento_confirmada varchar(500), nome_mae_confirmado varchar(500), endereco_confirmado varchar(500), nome_enr varchar(500), nascimento_enr varchar(500), nome_mae_enr varchar(500), logr_tipo1_enr varchar(500), logr_titulo1_enr varchar(500), logr_nome1_enr varchar(500), logr_numero1_enr varchar(500), logr_complemento1_enr varchar(500), endereco1_enr varchar(500), bairro1_enr varchar(500), cidade1_enr varchar(500), uf1_enr varchar(500), cep1_enr varchar(500), status_cpf varchar(500), data_hora_consulta varchar(500), cbo_enr varchar(500), cbo_descricao_enr varchar(500), faixa_renda_enr varchar(500) );
INSERT INTO #tb_primeirahigienizacaopf
SELECT  *
FROM analytics_sandbox.tb_primeirahigienizacaopfparte1;


--tratando a segunda tabela 
 drop TABLE IF EXISTS #tb_primeirahigienizacaopf2;

CREATE TABLE #tb_primeirahigienizacaopf2( cpf int8, nome varchar(500), datanascimento varchar(500), nomemae varchar(500), cep int8, logradouro varchar(500), numero varchar(500), complemento varchar(500), cidade varchar(500), bairro varchar(500), statusdareceitaonline varchar(500), profissao varchar(500), renda varchar(500), cpf_padr varchar(500), nome_padr varchar(500), nomemae_padr varchar(500), nasc_padr varchar(500), logr_tipo1_padr varchar(500), logr_titulo1_padr varchar(500), logr_nome1_padr varchar(500), logr_numero1_padr varchar(500), logr_complemento1_padr varchar(500), endereco1_padr varchar(500), bairro1_padr varchar(500), cidade1_padr varchar(500), uf1_padr varchar(500), cep1_padr varchar(500), nome_confirmado varchar(500), data_nascimento_confirmada varchar(500), nome_mae_confirmado varchar(500), endereco_confirmado varchar(500), nome_enr varchar(500), nascimento_enr varchar(500), nome_mae_enr varchar(500), logr_tipo1_enr varchar(500), logr_titulo1_enr varchar(500), logr_nome1_enr varchar(500), logr_numero1_enr varchar(500), logr_complemento1_enr varchar(500), endereco1_enr varchar(500), bairro1_enr varchar(500), cidade1_enr varchar(500), uf1_enr varchar(500), cep1_enr varchar(500), status_cpf varchar(500), data_hora_consulta varchar(500), cbo_enr varchar(500), cbo_descricao_enr varchar(500), faixa_renda_enr varchar(500) );
INSERT INTO #tb_primeirahigienizacaopf2
SELECT  *
FROM analytics_sandbox.tb_primeirahigienizacaopfparte2;


--criando uma terceira tabela e unindo AS 2 anteriores 
 drop TABLE IF EXISTS analytics_work.tbl_primeirahigienizacaopfFULL;

CREATE TABLE analytics_work.tbl_primeirahigienizacaopfFULL ( cpf int8, nome varchar(500), datanascimento varchar(500), nomemae varchar(500), cep int8, logradouro varchar(500), numero varchar(500), complemento varchar(500), cidade varchar(500), bairro varchar(500), statusdareceitaonline varchar(500), profissao varchar(500), renda varchar(500), cpf_padr varchar(500), nome_padr varchar(500), nomemae_padr varchar(500), nasc_padr varchar(500), logr_tipo1_padr varchar(500), logr_titulo1_padr varchar(500), logr_nome1_padr varchar(500), logr_numero1_padr varchar(500), logr_complemento1_padr varchar(500), endereco1_padr varchar(500), bairro1_padr varchar(500), cidade1_padr varchar(500), uf1_padr varchar(500), cep1_padr varchar(500), nome_confirmado varchar(500), data_nascimento_confirmada varchar(500), nome_mae_confirmado varchar(500), endereco_confirmado varchar(500), nome_enr varchar(500), nascimento_enr varchar(500), nome_mae_enr varchar(500), logr_tipo1_enr varchar(500), logr_titulo1_enr varchar(500), logr_nome1_enr varchar(500), logr_numero1_enr varchar(500), logr_complemento1_enr varchar(500), endereco1_enr varchar(500), bairro1_enr varchar(500), cidade1_enr varchar(500), uf1_enr varchar(500), cep1_enr varchar(500), status_cpf varchar(500), data_hora_consulta varchar(500), cbo_enr varchar(500), cbo_descricao_enr varchar(500), faixa_renda_enr varchar(500) );
INSERT INTO analytics_work.tbl_primeirahigienizacaopfFULL
SELECT  distinct *
FROM #tb_primeirahigienizacaopf
UNION
SELECT  distinct *
FROM #tb_primeirahigienizacaopf2;


--validacoes inicais 
SELECT  COUNT(*)
FROM analytics_work.tbl_primeirahigienizacaopfFULL;
*/

-------------------------------
-- INICIO DO ENRIQUECIMENTO 
-------------------------------


-------------------------------
-- BASES LEGADAS BKOxSERASA
-------------------------------


DROP TABLE IF EXISTS #tbl_cadastralpf1;
select 
a.*,
LOWER(NULLIF(coalesce(a.faixaderendaserasa, b.faixa_renda_enr),'')) as faixaderendaserasa2,
LOWER(NULLIF(coalesce(a.ocupacaoserasa, b.cbo_descricao_enr),''))  as ocupacaoserasa2,
CASE
WHEN b.status_cpf in ('REGULAR', 'PENDENTE DE REGULARIZACAO','TITULAR FALECIDO','SUSPENSA','CANCELADA')
THEN LOWER(coalesce(a.statuscpfserasa, b.status_cpf))
ELSE NULL
END statuscpfserasa2,
LOWER(NULLIF(coalesce(a.cepserasa, b.cep1_padr),''))  as cepserasa2
INTO #tbl_cadastralpf1 
from analytics_work.tbl_cadastralpf as a
left join analytics_work.tbl_primeirahigienizacaopfFULL as b
on a.documento = b.cpf;


ALTER TABLE #tbl_cadastralpf1 DROP COLUMN faixaderendaserasa;
ALTER TABLE #tbl_cadastralpf1 DROP COLUMN ocupacaoserasa;
ALTER TABLE #tbl_cadastralpf1 DROP COLUMN statuscpfserasa;
ALTER TABLE #tbl_cadastralpf1 DROP COLUMN cepserasa;

ALTER TABLE #tbl_cadastralpf1 RENAME COLUMN faixaderendaserasa2 TO faixaderendaserasa;
ALTER TABLE #tbl_cadastralpf1 RENAME COLUMN ocupacaoserasa2 TO ocupacaoserasa;
ALTER TABLE #tbl_cadastralpf1 RENAME COLUMN statuscpfserasa2 TO statuscpfserasa;
ALTER TABLE #tbl_cadastralpf1 RENAME COLUMN cepserasa2 TO cepserasa;

DROP TABLE IF EXISTS #tbl_cadastralpf;
SELECT
    documento,
    clienteid,
    nome,
    datanascimento,
    sexo,
    estadocivil,
    nomemae,
    enderecoid,
    cep,
    uf,
    cidade,
    bairro,
    logradouro,
    dddcelular,
    celular,
    email,
    faixaderendaserasa,
    ocupacaoserasa,
    statuscpfserasa,
    escolaridade,
    cepserasa,
    rendacepibge,
    pedagiopreferido,
    segmentoestacionamentopreferido,
    estacionamentopreferido,
    meiodepagamentopreferido,
    qtdadesoes,
    latitude,
    longitude,
    bancoemissorcartaodecreditoatual,
    bancoemissormaisutilizado
INTO #tbl_cadastralpf
FROM #tbl_cadastralpf1
;

DROP TABLE IF EXISTS #tbl_cadastralpf1;
SELECT * FROM #tbl_cadastralpf

