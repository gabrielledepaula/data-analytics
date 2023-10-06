DROP MATERIALIZED VIEW  if exists cntcar_ds_work.v01_customer_financial_history;
CREATE MATERIALIZED VIEW cntcar_ds_work.v01_customer_financial_history AS (

---------------- TRANSACTIONS MADE BY CUSTOMERS WHO NEED A LOAN  -------------------------
WITH TransactionWithLoan1 AS (
    SELECT
        a.ExtratoID
        ,a.AdesaoID
        ,a.DataTransacao
        ,a.ValorD
        ,a.SaldoAnteriorD
        ,a.TipoOperacaoID
    FROM "cntcar_work"."tb_conectcar_dbo_extrato_stage" a
    WHERE
        SaldoAnteriorD <= 0
        AND a.TipoOperacaoID IN (7, 14, 22, 23, 43)
        AND DATE_PART(YEAR, a.DataTransacao) >= 2019 -- WRITE HERE THE START PERIOD OF HISTORY
),
 
TransactionWithLoan AS (
    SELECT
        b.ClienteID
        ,COUNT(1) AS Debits
        ,SUM(CASE WHEN a.TipoOperacaoID IN (7, 22) THEN 1 ELSE 0 END) AS Debits_HIGHWAY
        ,SUM(CASE WHEN a.TipoOperacaoID = 22 THEN 1 ELSE 0 END) AS Debits_PARKING_LOT
        ,SUM(CASE WHEN a.TipoOperacaoID IN (23, 43) THEN 1 ELSE 0 END) AS Debits_MEMBERSHIPSUBSCRIPTION
    FROM TransactionWithLoan1 a
    INNER JOIN "cntcar_work"."tb_dados_ciclodevida_ativacao_stage" b ON a.AdesaoID = b.AdesaoID
    GROUP BY b.ClienteID
),
 
 
--------------- PAYMENT OF TRANSACTIONS THAT REQUIRED A LOAN --------------
LoanPayments AS (
    SELECT
        a.ExtratoID
        ,a.AdesaoID
        ,a.DataTransacao
        ,a.ValorD
        ,a.SaldoAnteriorD
    FROM "cntcar_work"."tb_conectcar_dbo_extrato_stage" a
    INNER JOIN (SELECT DISTINCT a.AdesaoID FROM TransactionWithLoan1 a) AS b ON a.AdesaoID = b.AdesaoID
    WHERE
        a.ValorD > 0
        AND a.SaldoAnteriorD < 0
        AND DATE_PART(YEAR, a.DataTransacao) >= 2019 -- WRITE HERE THE START PERIOD OF HISTORY
),
 
 
---------- CALCULATES THE AVERAGE OF DAYS TO PAY THE LOAN ------------------
AvarageDaysToPayBack1 AS (
    SELECT
        a.ExtratoID AS ExtratoIDDebit
        ,MIN(b.ExtratoID) AS ExtratoIDPayment
    FROM TransactionWithLoan1 a
    INNER JOIN LoanPayments b ON a.AdesaoID = b.AdesaoID
    WHERE a.ExtratoID < b.ExtratoID
    GROUP BY ExtratoIDDebit 
),
 
AvarageDaysToPayBack2 AS (
    SELECT
        b.AdesaoID
        ,a.ExtratoIDDebit
        ,ROW_NUMBER() OVER (PARTITION BY b.AdesaoID, a.ExtratoIDPayment ORDER BY b.AdesaoID ASC, a.ExtratoIDDebit) AS OrdemExtrato
        ,b.DataTransacao AS DateDebit
        ,a.ExtratoIDPayment
        ,c.DataTransacao AS DatePayment
    FROM AvarageDaysToPayBack1 a
    INNER JOIN TransactionWithLoan1 b ON a.ExtratoIDDebit = b.ExtratoID
    INNER JOIN LoanPayments c ON a.ExtratoIDPayment = c.ExtratoID
),
 
AvarageDaysToPayBack AS (
    SELECT
        b.ClienteID
        ,AVG(DATEDIFF(DAYS, a.DateDebit, a.DatePayment)) AS AvgDaysPayBack
    FROM AvarageDaysToPayBack2 a
    INNER JOIN "cntcar_work"."tb_dados_ciclodevida_ativacao_stage" b ON a.AdesaoID = b.AdesaoID
    WHERE a.OrdemExtrato = 1
    GROUP BY b.ClienteID
),
 
 
------------- TOTAL TRANSACTIONS DONE BY CUSTOMERS ---------------
TotalTransactions AS (
    SELECT
        b.ClienteID
        ,COUNT(1) AS Transactions
        ,SUM(CASE WHEN a.TipoOperacaoID IN (7, 22) THEN 1 ELSE 0 END) AS transactions_HIGHWAY
        ,SUM(CASE WHEN a.TipoOperacaoID = 22 THEN 1 ELSE 0 END) AS transactions_PARKING_LOT
        ,SUM(CASE WHEN a.TipoOperacaoID IN (23, 43) THEN 1 ELSE 0 END) AS transactions_MEMBERSHIPSUBSCRIPTION
    FROM "cntcar_work"."tb_conectcar_dbo_extrato_stage" a
    INNER JOIN "cntcar_work"."tb_dados_ciclodevida_ativacao_stage" b ON a.AdesaoID = b.AdesaoID
    WHERE
        a.TipoOperacaoID IN (7, 14, 22, 23, 43)
        AND DATE_PART(YEAR, a.DataTransacao) >= 2019 -- WRITE HERE THE START PERIOD OF HISTORY
        GROUP BY b.ClienteID
),
 
 
---------- AVERAGE CUSTOMER BALANCE ------------
AvarageFinancialHistory AS (
    SELECT
        b.ClienteID
        ,AVG(a.SaldoAnteriorD) AS AvarageBalance
        ,AVG(CASE WHEN a.TipoOperacaoID = 1 THEN a.ValorD ELSE NULL END) AS AvarageRecharge
    FROM "cntcar_work"."tb_conectcar_dbo_extrato_stage" a
    INNER JOIN "cntcar_work"."tb_dados_ciclodevida_ativacao_stage" b ON a.AdesaoID = b.AdesaoID
    WHERE DATE_PART(YEAR, a.DataTransacao) >= 2019 -- WRITE HERE THE START PERIOD OF HISTORY
    GROUP BY b.ClienteID
)
 
SELECT
    a.ClienteID
    ,b.Debits AS TotalLoans -- 1.   How many times the client has generated a credit with ConnecCar?
    ,c.AvgDaysPayBack -- 2.            Average time to repay (days)?
    ,b.Debits_HIGHWAY AS TotalLoans_HighWay -- 3.            How many times he has generated a credit for highways?
    ,b.Debits_PARKING_LOT AS TotaLoans_ParkingLot -- 4. How many times he has generated a credit for parking lots?
    ,b.Debits_MEMBERSHIPSUBSCRIPTION AS TotalLoans_MembershipSubscription -- 10.   How many times he has generated a credit for customer membership subscriptions?
    ,d.Transactions AS TotalTransactions -- 5.             How many transactions he has made (successful or unsuccessful)?
    ,d.transactions_HIGHWAY AS TotalTransactions_HighWay -- 6.  How many times he has used the tag for highways?
    ,d.transactions_PARKING_LOT AS TotaTransactions_ParkingLot -- 7.       How many times he has used the tag for parking lots?
    ,d.transactions_MEMBERSHIPSUBSCRIPTION AS TotalTransactions_MembershipSubscription -- 11.         How many times he has used the tag for customer membership subscriptions?
    ,e.AvarageBalance -- 8.             Average balance (in the entire year)?
    ,e.AvarageRecharge -- 9. Average recharge (in the entire year)?
    ,f.data_nascimento -- CUSTOMER BIRTH DATE
    ,DATEDIFF(YEAR, f.data_nascimento, getdate()) AS CustomerAge -- AGE
    ,f.estadocivilid -- MARITAL STATUS
    ,f.estado -- ESTATE
FROM AvarageFinancialHistory a
LEFT JOIN TransactionWithLoan b ON a.ClienteID = b.ClienteID
LEFT JOIN AvarageDaysToPayBack c ON a.ClienteID = c.ClienteID
LEFT JOIN TotalTransactions d ON a.ClienteID = d.ClienteID
LEFT JOIN AvarageFinancialHistory e ON a.ClienteID = e.ClienteID
INNER JOIN "cntcar_work"."tb_dados_ciclodevida_cliente_stage" f ON a.ClienteID = f.ClienteID
    
)