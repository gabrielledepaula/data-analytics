DROP MATERIALIZED VIEW  if exists cntcar_ds_work.v01_customer_last_transaction;
CREATE MATERIALIZED VIEW cntcar_ds_work.v01_customer_last_transaction AS (

WITH 
LastTransactionDetails AS ( -- INFORMATIONS RELATED TO CUSTOMER LAST TRANSACTION
    SELECT
        a.ClienteID -- CUSTOMER UNIQUE ID
        ,a.transacaoid -- UNIQUE IDENTIFIER ID OF THE TRANSACTION
        ,b.Data -- DATE THAT THE TRANSACTION OCURRED
        ,b.adesaoid -- TAG OF THE CAR USED IN LAST TRANSACTION. ONE CUSTOMER CAN HAVE MULTIPLIE TAGS
        ,b.valor -- TOTAL TRANSACTION VALUE
        ,b.TipoOperacaoID 
        ,c.SaldoAnteriorD -- BALANCE BEFORE TRANSACTION
        ,b.SaldoID -- ACCOUNT BALANCE UNIQUE IDENTIFIER ID
        ,CASE WHEN c.SaldoAnteriorD < b.Valor THEN 'NO' ELSE 'YES' END AS HadCreditAvailabel -- 1.The client had a credit with connect car at the moment? (yes or no)
    	,c.Placa
    FROM (
    	SELECT
    		MAX(a.transacaoid) AS transacaoid
    		,b.ClienteID
      	FROM "cntcar_work"."tb_conectcar_dbo_transacao_stage" a
      	INNER JOIN "cntcar_work"."tb_dados_ciclodevida_ativacao_stage" b ON a.AdesaoID = b.AdesaoID
      	WHERE a.TipoOperacaoID IN (7, 14, 22, 23, 43)
      	GROUP BY
      		b.ClienteID ) AS a	
    INNER JOIN "cntcar_work"."tb_conectcar_dbo_transacao_stage" b ON a.transacaoid = b.TransacaoID
    INNER JOIN "cntcar_work"."tb_conectcar_dbo_extrato_stage" c ON a.TransacaoID = c.TransacaoID
),

DaysWithAvailabelCredit AS ( -- CALCULATE THE AMOUNT OF TIME SINCE CUSTOMER HAS CREDIT AVAILABEL
    SELECT 
    	a.AdesaoID
    	,CASE 
      		WHEN b.AdesaoID IS NULL OR HadCreditAvailabel = 'NO' THEN 0 
      		ELSE DATEDIFF(DAY, c.DataTransacao, a.Data) 
      		END AS DaysWithCreditAvailabel -- 2.If he has a credit, since when? (days)
    	,c.DataTransacao
    FROM LastTransactionDetails a
    LEFT JOIN (
        SELECT
            MAX(a.ExtratoID) ExtratoID
            ,a.AdesaoID
    	FROM "cntcar_work"."tb_conectcar_dbo_extrato_stage" a
    	INNER JOIN LastTransactionDetails b ON a.AdesaoID = b.AdesaoID AND b.Data > a.DataTransacao
        WHERE a.SaldoAnteriorD <=0 
        GROUP BY a.AdesaoID ) AS b ON a.AdesaoID = b.AdesaoID
    INNER JOIN "cntcar_work"."tb_conectcar_dbo_extrato_stage" c ON b.ExtratoID = c.ExtratoID
),



VehicleInformation AS (
    SELECT DISTINCT
    	a.AdesaoID
    	,b.Eixos -- 6. How many axes?
    	,c.Classificacao -- 5.	What vehicle he was using (leve / heavy)?
    FROM (SELECT DISTINCT AdesaoID, Placa FROM LastTransactionDetails) AS a
    LEFT JOIN "cntcar_work"."tb_conectcar_dbo_veiculo_stage" b ON a.Placa = b.Placa
    LEFT JOIN "cntcar_work"."tb_conectcar_dbo_categoriaveiculo_stage" c ON b.categoria_id = c.categoria_veiculo_id
),

ActualBalance AS (
    SELECT DISTINCT
    	b.AdesaoID
    	,a.Valor_D 
    FROM "cntcar_work"."tb_conectcar_dbo_saldo_stage" a
    INNER JOIN "cntcar_work"."tb_dados_ciclodevida_ativacao_stage" b ON a.Saldo_ID = b.SaldoID
),

LastRecharge AS (
    SELECT
    	a.AdesaoID
    	,b.ValorD
    	,b.datatransacao
    FROM (SELECT
          	MAX(a.extratoid) AS extratoid
          	,a.adesaoid
      	  FROM "cntcar_work"."tb_conectcar_dbo_extrato_stage" a
      	  WHERE a.TipoOperacaoID = 1
      	  GROUP BY a.adesaoid ) AS a
    INNER JOIN "cntcar_work"."tb_conectcar_dbo_extrato_stage" b ON a.ExtratoID = b.ExtratoID
)


SELECT
    a.ClienteID -- CUSTOMER UNIQUE ID
    ,a.transacaoid -- UNIQUE IDENTIFIER ID OF THE TRANSACTION
    ,a.Data AS LastTransactionDate -- DATE THAT THE TRANSACTION OCURRED
    ,a.adesaoid -- TAG OF THE CAR USED IN LAST TRANSACTION. ONE CUSTOMER CAN HAVE MULTIPLIE TAGS
    ,a.valor AS LastTransactionValue-- TOTAL TRANSACTION VALUE
    ,a.SaldoID -- ACCOUNT BALANCE UNIQUE IDENTIFIER ID
    ,a.HadCreditAvailabel -- 1.The client had a credit with connect car at the moment? (yes or no)
    ,b.DaysWithCreditAvailabel -- 2. If he has a credit, since when? (days)
    ,a.SaldoAnteriorD AS BalanceBeforeLastTransaction -- 3.	If he has a credit, by how much is this credit?
	,a.TipoOperacaoID -- 4.	If he has a credit, what caused the credit (highway or parking lot) || 7 AND 22 = HIGHWAY / 14 = PARKING LOT // 23, 43 = MEMBERSHIP
	,CASE 
    	WHEN a.TipoOperacaoID IN (7, 22) THEN 'HIGHWAY'
 		WHEN a.TipoOperacaoID = 22 THEN 'PARKING_LOT'
  		WHEN a.TipoOperacaoID IN (23, 43) THEN 'MEMBERSHIP SUBSCRIPTION' END AS OperationType
	,c.Classificacao AS VehicleType -- 5.	What vehicle he was using (leve / heavy)?
    ,c.Eixos AS Axes -- 6.	How many axes?
    ,d.Valor_D AS ActualBalance-- 7.	What was his balance at the moment?
    ,e.ValorD AS LastRechargeValue -- 8.	What was his last recharge?
    ,e.datatransacao LastRechargeDate -- 8.	What was his last recharge?
    ,CASE WHEN h.pagamentocartao = TRUE THEN 'CREDIT CARD' ELSE 'OTHERS' END AS MethodOfPayment -- 9. What is his method of payment at the moment?
    ,CASE WHEN a.Valor > a.SaldoAnteriorD THEN DATEDIFF(DAY, a.Data, GETDATE()) ELSE 0 END AS DaysOwningLoan -- HOW LONG DOES THE CUSTOMER OWN CONECTCAR MONEY
    ,CASE 
    	WHEN a.HadCreditAvailabel = 'NO' AND d.Valor_D >= 0 THEN 'YES' 
        WHEN a.HadCreditAvailabel = 'NO' AND d.Valor_D <= 0 THEN 'NO'
        ELSE 'HAD NO DEBITS ON HIS LAST TRANSACTION'
        END AS DoesStillHasDebit -- DOES THE CUSTOMER STILL HAS DEBITS?
    ,f.data_nascimento -- CUSTOMER BIRTH DATE
    ,DATEDIFF(YEAR, f.data_nascimento, getdate()) AS CustomerAge -- AGE
    ,f.sexoid -- SEX ID
    ,f.estadocivilid -- MARITAL STATUS
    ,f.estado -- ESTATE
    ,f.pessoa_fisica -- TRUE fisica FALSE company
    ,g.nomeplano --SUBSCRIPTION TENURE
FROM LastTransactionDetails a
LEFT JOIN DaysWithAvailabelCredit b ON a.AdesaoID = b.AdesaoID
LEFT JOIN VehicleInformation c ON a.AdesaoID = c.AdesaoID
LEFT JOIN ActualBalance d ON a.AdesaoID = d.AdesaoID
LEFT JOIN LastRecharge e ON a.AdesaoID = e.AdesaoID
INNER JOIN "cntcar_work"."tb_dados_ciclodevida_cliente_stage" f ON a.ClienteID = f.ClienteID
INNER JOIN "cntcar_work"."tb_dados_ciclodevida_ativacao_stage" g ON a.AdesaoID = g.AdesaoID
LEFT JOIN "cntcar_work"."tb_conectcar_dbo_recarga_stage" h ON a.transacaoid = h.TransacaoID
WHERE DATE_PART(YEAR, a.Data) >= 2019 -- WRITE HERE THE START PERIOD OF HISTORY ;  