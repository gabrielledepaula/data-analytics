-- JOINING LAST TRANSACTION TO CUSTOMER FINANCIAL HISTORY
DROP MATERIALIZED VIEW  if exists cntcar_ds_work.v02_customer_union;
CREATE MATERIALIZED VIEW cntcar_ds_work.v02_customer_union AS (

SELECT
    -- LAST TRANSACTION
    tab_last_transaction.ClienteID -- CUSTOMER UNIQUE ID
    ,tab_last_transaction.transacaoid -- UNIQUE IDENTIFIER ID OF THE TRANSACTION
    ,tab_last_transaction.LastTransactionDate -- DATE THAT THE TRANSACTION OCURRED
    ,tab_last_transaction.adesaoid -- TAG OF THE CAR USED IN LAST TRANSACTION. ONE CUSTOMER CAN HAVE MULTIPLIE TAGS
    ,tab_last_transaction.LastTransactionValue-- TOTAL TRANSACTION VALUE
    ,tab_last_transaction.SaldoID -- ACCOUNT BALANCE UNIQUE IDENTIFIER ID
    ,tab_last_transaction.HadCreditAvailabel -- 1.The client had a credit with connect car at the moment? (yes or no)
    ,tab_last_transaction.DaysWithCreditAvailabel -- 2. If he has a credit, since when? (days)
    ,tab_last_transaction.BalanceBeforeLastTransaction -- 3. If he has a credit, by how much is this credit?
    ,tab_last_transaction.TipoOperacaoID -- 4. If he has a credit, what caused the credit (highway or parking lot) || 7 AND 22 = HIGHWAY / 14 = PARKING LOT // 23, 43 = MEMBERSHIP
    ,tab_last_transaction.OperationType
    ,tab_last_transaction.VehicleType -- 5.   What vehicle he was using (leve / heavy)?
    ,tab_last_transaction.Axes -- 6.  How many axes?
    ,tab_last_transaction.ActualBalance-- 7. What was his balance at the moment?
    ,tab_last_transaction.LastRechargeValue -- 8.    What was his last recharge?
    ,tab_last_transaction.LastRechargeDate -- 8. What was his last recharge?
    ,tab_last_transaction.MethodOfPayment -- 9. What is his method of payment at the moment?
    ,tab_last_transaction.DaysOwningLoan -- HOW LONG DOES THE CUSTOMER OWN CONECTCAR MONEY
    ,tab_last_transaction.DoesStillHasDebit -- DOES THE CUSTOMER STILL HAS DEBITS?
    ,tab_last_transaction.data_nascimento -- CUSTOMER BIRTH DATE
    ,tab_last_transaction.CustomerAge -- AGE
    ,tab_last_transaction.sexoid -- SEX
    ,tab_last_transaction.estadocivilid -- MARITAL STATUS
    ,tab_last_transaction.estado -- ESTATE
    ,tab_last_transaction.nomeplano --SUBSCRIPTION TENURE
    ,tab_last_transaction.pessoa_fisica -- TYPE OF CUSTOMERS

    --FINANCIAL HISTORY
    ,tab_financial_history.TotalLoans -- 1. How many times the client has generated a credit with ConnecCar?
    ,tab_financial_history.AvgDaysPayBack -- 2. Average time to repay (days)?
    ,tab_financial_history.TotalLoans_HighWay -- 3. How many times he has generated a credit for highways?
    ,tab_financial_history.TotaLoans_ParkingLot -- 4.   How many times he has generated a credit for parking lots?
    ,tab_financial_history.TotalLoans_MembershipSubscription -- 10. How many times he has generated a credit for customer membership subscriptions?
    ,tab_financial_history.TotalTransactions -- 5.  How many transactions he has made (successful or unsuccessful)?
    ,tab_financial_history.TotalTransactions_HighWay -- 6.  How many times he has used the tag for highways?
    ,tab_financial_history.TotaTransactions_ParkingLot -- 7.    How many times he has used the tag for parking lots?
    ,tab_financial_history.TotalTransactions_MembershipSubscription -- 11.  How many times he has used the tag for customer membership subscriptions?
    ,tab_financial_history.AvarageBalance -- 8. Average balance (in the entire year)?
    ,tab_financial_history.AvarageRecharge -- 9. Average recharge (in the entire year)?
    --,tab_financial_history.data_nascimento -- CUSTOMER BIRTH DATE
    --,tab_financial_history.CustomerAge -- AGE
    --,tab_financial_history.estadocivilid -- MARITAL STATUS
    --,tab_financial_history.estado -- ESTATE
FROM "cntcar_ds_work"."v01_customer_last_transaction" AS tab_last_transaction
INNER JOIN "cntcar_ds_work"."v01_customer_financial_history" AS tab_financial_history 
ON tab_last_transaction.clienteid = tab_financial_history.clienteid
  
)