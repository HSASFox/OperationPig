--####################################################################################################################################
-- 1. Create external a_rxclaims and a_rxclaimsamt tables
--####################################################################################################################################
-- CREATE EXTERNAL TABLE wha.a_rxclaims
-- (
-- insuredmemberidentifier STRING, claimidentifier STRING, claimprocesseddatetime STRING, prescriptionfilldate STRING, issuerclaimpaiddate STRING, prescriptionservicereferencenumber STRING, nationaldrugcode STRING, dispensingprovideridqualifier STRING, dispensingprovideridentifier STRING, prescriptionfillnumber INT, dispensingstatuscode STRING, voidreplacecode STRING, allowedtotalcostamount DOUBLE, policypaidamount DOUBLE, derivedserviceclaimindicator STRING, prescribingprovideridentifier STRING, importmonthnumber STRING, importdate STRING, iscurrent INT
-- ) 
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION '/user/sfox/WHA_A_pharmacyclaims.txt'
-- ;
-- CREATE EXTERNAL TABLE wha.a_rxclaimsamt
-- (
-- insuredmemberidentifier STRING, claimidentifier STRING, allowedtotalamount DOUBLE, policypaidtotalamount DOUBLE, servicefromdate STRING, servicetodate STRING, importmonthnumber STRING, importdate STRING, iscurrent INT
-- )
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION '/user/sfox/WHA_A_pharmacyclaims_amt.txt'
-- ;
--####################################################################################################################################
-- 2. Set insurplanid variable, apply settings to optimize processing, and drop RX "B" tables if they exist
--####################################################################################################################################
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
DROP TABLE IF EXISTS wha.B_003_PharmacyClaimDetail;
DROP TABLE IF EXISTS wha.B_003a_PharmacyClaimAmount;
SET insurplanid = 'IPlan';
--####################################################################################################################################
-- 3. Create RX "B" tables, stored as ORC (optimized row columnar) format
-- ORC format information: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+ORC
--####################################################################################################################################
CREATE TABLE IF NOT EXISTS wha.B_003_PharmacyClaimDetail
STORED AS ORC tblproperties("orc.compress"="SNAPPY")
AS
SELECT
${hiveconf:insurplanid} AS insuranceplanidentifier, a.insuredmemberidentifier, a.claimidentifier, a.claimprocesseddatetime, a.prescriptionfilldate, a.issuerclaimpaiddate AS insurerclaimpaiddate, a.prescriptionservicereferencenumber, a.nationaldrugcode, a.dispensingprovideridqualifier, a.dispensingprovideridentifier, a.prescriptionfillnumber, a.dispensingstatuscode, a.voidreplacecode, a.allowedtotalcostamount, a.policypaidamount, a.derivedserviceclaimindicator, b.recordidentifier AS memberreckey
FROM wha.a_rxclaims a
INNER JOIN wha.B_000_InsuredMember b ON a.insuredmemberidentifier = b.insuredmemberidentifier
WHERE a.claimidentifier NOT IN (SELECT DISTINCT claimidentifier FROM wha.a_rxclaims WHERE voidreplacecode = 'V' AND YEAR(prescriptionfilldate) >= '2014')
GROUP BY a.insuredmemberidentifier, a.claimidentifier, a.claimprocesseddatetime, a.prescriptionfilldate, a.issuerclaimpaiddate, a.prescriptionservicereferencenumber, a.nationaldrugcode, a.dispensingprovideridqualifier, a.dispensingprovideridentifier, a.prescriptionfillnumber, a.dispensingstatuscode, a.voidreplacecode, a.allowedtotalcostamount, a.policypaidamount, a.derivedserviceclaimindicator, b.recordidentifier
;
CREATE TABLE IF NOT EXISTS wha.B_003a_PharmacyClaimAmount
STORED AS ORC tblproperties("orc.compress"="SNAPPY")
AS
SELECT DISTINCT
a.insuredmemberidentifier, a.claimidentifier, a.allowedtotalamount, a.policypaidtotalamount, a.servicefromdate, a.servicetodate, b.recordidentifier AS memberreckey,YEAR(a.servicefromdate) AS claimyear, MONTH(a.servicefromdate) AS claimmonth
FROM wha.a_rxclaimsamt a
INNER JOIN wha.B_000_InsuredMember b ON a.insuredmemberidentifier = b.insuredmemberidentifier
WHERE a.claimidentifier NOT IN (SELECT DISTINCT claimidentifier FROM wha.a_rxclaims WHERE voidreplacecode = 'V' AND YEAR(prescriptionfilldate) >= '2014')
;
INSERT OVERWRITE LOCAL DIRECTORY '/grid/0/nfs/user/sfox/output/WHA/B_003_PharmacyClaimDetail' ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' SELECT insuranceplanidentifier,insuredmemberidentifier,claimidentifier,TO_DATE(claimprocesseddatetime) AS claimprocesseddatetime, prescriptionfilldate,TO_DATE(insurerclaimpaiddate) AS insurerclaimpaiddate,  prescriptionservicereferencenumber, nationaldrugcode, dispensingprovideridqualifier,dispensingprovideridentifier,prescriptionfillnumber,dispensingstatuscode,voidreplacecode,CAST(allowedtotalcostamount AS STRING) AS allowedtotalcostamount, CAST(policypaidamount AS STRING) AS policypaidamount, derivedserviceclaimindicator, memberreckey FROM wha.B_003_PharmacyClaimDetail
;
INSERT OVERWRITE LOCAL DIRECTORY '/grid/0/nfs/user/sfox/output/WHA/B_003a_PharmacyClaimAmount' ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' SELECT insuredmemberidentifier, claimidentifier, CAST(allowedtotalamount AS STRING) AS allowedtotalamount, CAST(policypaidtotalamount AS STRING) AS policypaidtotalamount, servicefromdate, servicetodate, memberreckey, claimyear, claimmonth FROM wha.B_003a_PharmacyClaimAmount
;
