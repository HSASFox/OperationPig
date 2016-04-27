-- 1. CREATE TABLE within wha database
-- CREATE EXTERNAL TABLE wha.a_memberinfo
-- (
-- insuredmemberidentifier INT, insuredmemberbirthdate STRING, insuredmembergendercode STRING, subscriberindicator STRING, subscriberidentifier INT, insuranceplanidentifier STRING, coveragestartdate STRING, coverageenddate STRING, enrollmentmaintenancetypecode STRING, insuranceplanpremiumamount DOUBLE, rateareaidentifier STRING, client STRING,legalentity STRING, issuerstate STRING, issuer STRING, policyriskpool STRING, policyproduct STRING, metallevel STRING, mbrzipcode STRING, mbrstate STRING, channel STRING, insuredmemberlifestage STRING, insuredmemberincome INT, csr STRING, newmemberindicator STRING, edgeserversubmissiondate STRING, grandfatheredplanindicator STRING, riskadjustedindicator STRING, groupidentifier STRING, groupname STRING, subgroupidentifier STRING, subgroupname STRING, contractnumber STRING, relationship INT, membername STRING, hsaproductkey INT, hsariskpoolkey INT, hsalegalentitykey INT, hsarateareakey INT, hsalifestagekey INT, hsametallevelkey INT, hsamembergeokey INT, importmonthnumber INT, importdate STRING, iscurrent INT
-- ) 
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION '/user/sfox/WHA_A_memberinfo.txt'
-- ;
-- CREATE EXTERNAL TABLE wha.a_planinfo
-- (
-- client STRING, issuerlegalentity STRING, issuerstate STRING, insuranceplanidentifier STRING, issueridentifier STRING, planriskadjustmentmodel STRING, policyriskpool STRING, policyproducttype STRING, policychannel INT, policymetallevel STRING, policygrandfatheredindicator INT, policyriskadjustedindicator INT, policycsr STRING
-- )
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION '/user/sfox/WHA_A_planinfo.txt'
-- ;
-- CREATE TABLE reference.client
-- (
-- client STRING, clientkey INT
-- )
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE 
-- ;
-- CREATE TABLE reference.geoarea
-- (
-- geokey INT, zip STRING, city STRING, state STRING, areacode STRING, county STRING, msa STRING, pmsa STRING, cbsa STRING, cbsa_div STRING, cbsatype STRING,ratingareaidentifier STRING
-- )
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE 
-- ;
-- CREATE TABLE reference.referencegroup
-- (
-- groupid STRING, groupkey INT
-- )
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE
-- ;
-- CREATE TABLE reference.issuer
-- (
-- issuer STRING, issuerkey INT
-- )
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE
-- ;
-- CREATE TABLE reference.legalentity
-- (
-- legalentity STRING, legalentitykey INT
-- )
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE
-- ;
-- CREATE TABLE reference.lifestage
-- (
-- lifestage STRING, lifestagekey INT 
-- )
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE
-- ;
-- CREATE TABLE reference.metallevel
-- (
-- metallevelkey INT, metallevel STRING, selectionfactor DOUBLE, selectionfactormin DOUBLE, selectionfactormax DOUBLE, selectionfactorrange STRING, metallevelabbrev STRING
-- )
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE
-- ;
-- CREATE TABLE reference.product
-- (
-- product STRING, productkey INT
-- )
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE
-- ;
-- CREATE TABLE reference.ratearea
-- (
-- ratearea STRING, rateareakey INT
-- )
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE
-- ;
-- CREATE TABLE reference.riskpool
-- (
-- riskpool STRING, riskpoolkey INT
-- )
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE
-- ;
-- CREATE TABLE reference.riskadjustmentmodel
-- (
-- planriskadjustmentmodelkey INT, planriskadjustmentmodelname STRING
-- )
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE
-- ;
-- CREATE TABLE reference.state
-- (
-- state STRING, statekey INT
-- )
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE
-- ;
-- CREATE TABLE reference.state_arf
-- (
-- age DOUBLE, state STRING, arf DOUBLE, statekey DOUBLE, riskpoolkey DOUBLE
-- )
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE
-- ;
---- Load data into the tables above:
-- LOAD DATA INPATH 'hdfs:/user/sfox/Reference/Client.txt' INTO TABLE reference.client;
-- LOAD DATA LOCAL '/user/sfox/Reference/GeoArea.txt' INTO TABLE reference.geoarea;
-- LOAD DATA LOCAL '/user/sfox/Reference/State.txt' INTO TABLE reference.state;
-- LOAD DATA LOCAL '/user/sfox/Reference/RiskAdjustmentModel.txt' INTO TABLE reference.riskadjustmentmodel;
-- LOAD DATA LOCAL '/user/sfox/Reference/RiskPool.txt' INTO TABLE reference.riskpool;
-- LOAD DATA LOCAL '/user/sfox/Reference/RateArea.txt' INTO TABLE reference.ratearea;
-- LOAD DATA LOCAL '/user/sfox/Reference/Product.txt' INTO TABLE reference.product;
-- LOAD DATA LOCAL '/user/sfox/Reference/MetalLevel.txt' INTO TABLE reference.metallevel;
-- LOAD DATA LOCAL '/user/sfox/Reference/LifeStage.txt' INTO TABLE reference.lifestage;
-- LOAD DATA LOCAL '/user/sfox/Reference/LegalEntity.txt' INTO TABLE reference.legalentity;
-- LOAD DATA LOCAL '/user/sfox/Reference/Issuer.txt' INTO TABLE reference.issuer;
-- LOAD DATA LOCAL '/user/sfox/Reference/Group.txt' INTO TABLE reference.referencegroup;
-- LOAD DATA INPATH '/user/sfox/Reference/State_ARF.txt' INTO TABLE reference.state_arf;
-- execute from command line:
-- hive -f memberinfo.hql
--
-- START
-- Optimize query execution based on the suggestions found in the links below:
-- https://www.qubole.com/blog/big-data/hive-best-practices/?nabe=5695374637924352:1&utm_referrer=https%3A%2F%2Fwww.google.com
-- http://hortonworks.com/blog/5-ways-make-hive-queries-run-faster/
SET hive.execution.engine=tez;
SET mapred.compress.map.output=true;
SET mapred.output.compress=true;
SET hive.exec.parallel=true;
SET hive.enforce.bucketing=true;
SET hive.optimize.bucketmapjoin=true;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
DROP TABLE IF EXISTS wha.B_000_InsuredMember;
DROP TABLE IF EXISTS wha.B_001_InsuredMemberProfile;
DROP TABLE IF EXISTS wha.B_004_PlanInfo;
--####################################################################################################################################
-- 2. Create "L" plan lookup table
--####################################################################################################################################
CREATE TEMPORARY TABLE wha.plan
STORED AS ORC tblproperties("orc.compress"="SNAPPY")
AS
SELECT DISTINCT
RANK() OVER(ORDER BY a.insuranceplanidentifier, c.legalentitykey) AS plankey, a.insuranceplanidentifier AS insuranceplan, b.clientkey, c.legalentitykey, d.issuerkey, COALESCE(j.statekey,-1) AS issuerstatekey, COALESCE(h.planriskadjustmentmodelkey,-1) AS planriskadjustmentmodelkey, COALESCE(g.riskpoolkey,-1) AS riskpoolkey, COALESCE(e.productkey,-1) AS productkey, a.policychannel AS channelkey, COALESCE(i.metallevelkey,-1) AS metallevelkey, a.policyriskadjustedindicator AS riskadjustedindicator, a.policygrandfatheredindicator AS grandfatheredplanindicator, CURRENT_DATE() AS dateoflastupdate
FROM wha.a_planinfo a 
INNER JOIN reference.client b ON a.client = b.client
INNER JOIN reference.legalentity c ON a.issuerlegalentity = c.legalentity
INNER JOIN reference.issuer d ON a.issueridentifier = d.issuer
INNER JOIN reference.product e ON a.policyproducttype = e.product
LEFT JOIN reference.riskpool g ON a.policyriskpool = g.riskpool
LEFT JOIN reference.riskadjustmentmodel h ON a.planriskadjustmentmodel = h.planriskadjustmentmodelname
LEFT JOIN reference.metallevel i ON a.policymetallevel = i.metallevelabbrev
LEFT JOIN  reference.state j ON a.issuerstate = j.state
;
--####################################################################################################################################
-- 3. Create insured member "B" table
--####################################################################################################################################
CREATE TABLE IF NOT EXISTS wha.B_000_InsuredMember
STORED AS ORC tblproperties("orc.compress"="SNAPPY")
AS
SELECT
RANK() OVER(ORDER BY insuredmemberidentifier) AS recordidentifier,
insuredmemberidentifier,
MAX(insuredmemberbirthdate) AS insuredmemberbirthdate,
MAX(insuredmembergendercode) AS insuredmembergendercode
FROM wha.a_memberinfo
GROUP BY insuredmemberidentifier
;
--####################################################################################################################################
-- 4. Create insured member profile "B" table - stage I
--####################################################################################################################################
CREATE TEMPORARY TABLE wha.insuredmemberprofile
STORED AS ORC tblproperties("orc.compress"="SNAPPY")
AS
SELECT
d.issuerkey, a.subscriberindicator, a.subscriberidentifier, a.coveragestartdate, a.coverageenddate, COALESCE(m.PlanKey, -1) AS insuranceplanidentifier, a.enrollmentmaintenancetypecode, 0.00 AS insuranceplanpremiumamount, a.rateareaidentifier, a.csr, a.insuredmemberincome, a.Channel, a.issuerstate AS insurerstate, a.mbrstate, a.grandfatheredplanindicator, a.newmemberindicator, a.hsaproductkey, a.hsariskpoolkey, a.riskadjustedindicator, a.hsalegalentitykey, a.hsarateareakey, a.hsalifestagekey, a.hsametallevelkey, a.hsamembergeokey, b.recordidentifier AS memberRecKey, a.insuredmemberidentifier, a.groupidentifier, a.groupname, a.subgroupidentifier, a.subgroupname, a.contractnumber, a.relationship, a.membername, 
CASE 
WHEN a.RiskAdjustedIndicator = 1  AND b.recordidentifier IS NOT NULL AND YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(coveragestartdate,'MM/dd/yyyy'),'yyyy-MM-dd')) - YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(b.insuredmemberbirthdate,'MM/dd/yyyy'),'yyyy-MM-dd')) > 0 
THEN YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(coveragestartdate,'MM/dd/yyyy'),'yyyy-MM-dd')) - YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(b.insuredmemberbirthdate,'MM/dd/yyyy'),'yyyy-MM-dd')) ELSE 0 END AS ageatenrollment, 
0 AS memberpremiumamount, 0 AS billableMemberIndicator, -1 AS hsameaccepted, 1.00 AS GCF 
FROM wha.a_memberinfo a
INNER JOIN wha.B_000_InsuredMember b ON a.insuredMemberIdentifier = b.insuredMemberIdentifier
INNER JOIN reference.issuer d ON a.issuer = d.Issuer
INNER JOIN reference.client g ON a.client = g.Client
INNER JOIN reference.legalentity l ON a.legalEntity = l.LegalEntity
INNER JOIN reference.product i ON a.policyProduct = i.Product
LEFT JOIN reference.riskpool e ON a.policyRiskPool = e.RiskPool
LEFT JOIN reference.metallevel k ON a.metallevel = k.metallevelabbrev
LEFT JOIN  reference.state j ON a.issuerState = j.state
LEFT JOIN wha.plan m
ON 
COALESCE(g.clientkey, -1) = m.ClientKey 
AND COALESCE(l.LegalEntityKey, -1) = m.LegalEntityKey
AND COALESCE(d.IssuerKey, -1) = m.IssuerKey
AND COALESCE(a.insurancePlanIdentifier, -1) = m.InsurancePlan
AND COALESCE(i.ProductKey, -1) = m.ProductKey
AND COALESCE(e.RiskPoolKey, -1) = m.RiskPoolKey
AND COALESCE(k.MetalLevelKey, -1) = m.MetalLevelKey
AND COALESCE(j.StateKey, -1) = m.issuerStateKey
AND COALESCE(a.Channel, -1) = m.channelKey
AND COALESCE(a.RiskAdjustedIndicator, -1) = m.RiskAdjustedIndicator
AND COALESCE(a.grandfatheredPlanIndicator, -1) = m.grandfatheredPlanIndicator
;
--####################################################################################################################################
-- 5. Create premium_2 table
--####################################################################################################################################
CREATE TEMPORARY TABLE wha.premium_2
STORED AS ORC tblproperties("orc.compress"="SNAPPY")
AS
SELECT 
a.*, CASE WHEN subscriberchildagerank > 3 THEN 0.00 ELSE b.arf END AS arf
FROM
(
SELECT DISTINCT
a.subscriberIdentifier, a.memberRecKey, a.insuredMemberIdentifier, a.insurancePlanIdentifier, a.insurancePlanPremiumAmount, a.AgeAtEnrollment, b.SubscriberChildAgeRank, a.coverageStartDate, coverageEndDate, insurerstate, hsariskpoolkey
FROM wha.insuredmemberprofile a
LEFT JOIN
(SELECT 
subscriberidentifier, memberRecKey, insuranceplanidentifier, ageatenrollment, coveragestartdate, RANK() OVER(PARTITION BY subscriberidentifier, insuranceplanidentifier, coveragestartdate ORDER BY ageatenrollment DESC, memberreckey ) AS subscriberchildagerank
FROM wha.insuredmemberprofile
WHERE ageatenrollment < 21 AND riskadjustedindicator = 1 AND hsameaccepted = -1) AS b 
ON 
a.subscriberidentifier = b.subscriberidentifier
AND a.memberreckey = b.memberreckey
AND a.insurancePlanIdentifier = b.insuranceplanidentifier
AND a.coveragestartdate = b.coveragestartdate
WHERE riskadjustedindicator = 1 AND hsameaccepted = -1
) AS a
INNER JOIN reference.state_arf b 
ON 
a.ageatenrollment = b.age 
AND a.insurerstate = b.state
AND a.hsariskpoolkey = b.riskpoolkey
;
--####################################################################################################################################
-- 6. Create premium_3 table
--####################################################################################################################################
CREATE TEMPORARY TABLE wha.premium_3
STORED AS ORC tblproperties("orc.compress"="SNAPPY")
AS
SELECT subscriberidentifier, insuranceplanidentifier, coveragestartdate, coverageenddate, MAX(insuranceplanpremiumamount) AS premium
FROM wha.premium_2
WHERE insuredmemberidentifier = subscriberIdentifier
GROUP BY subscriberIdentifier, insurancePlanIdentifier, coveragestartdate, coverageEndDate
;
--####################################################################################################################################
-- 7. Create arf table
--####################################################################################################################################
CREATE TEMPORARY TABLE wha.arf
STORED AS ORC tblproperties("orc.compress"="SNAPPY")
AS
SELECT DISTINCT a.memberRecKey, a.subscriberIdentifier, a.insurancePlanIdentifier, a.coverageStartDate, a.coverageEndDate, (arf/ARFTotal)*b.premium AS premium, a.arf, b.arftotal
FROM wha.premium_2 a
INNER JOIN
(
SELECT b.subscriberidentifier, b.insuranceplanidentifier, b.coveragestartdate, b.coverageenddate, premium, SUM(arf) AS arftotal
FROM wha.premium_2 a
INNER JOIN wha.premium_3 b
ON
a.subscriberidentifier = b.subscriberidentifier 
AND a.insuranceplanidentifier = b.insuranceplanidentifier
AND a.coveragestartdate = b.coveragestartdate
AND a.coverageEndDate = b.coverageEndDate
GROUP BY b.subscriberidentifier, b.insuranceplanidentifier, b.coveragestartdate, b.coverageenddate, premium
) AS b
ON
a.subscriberidentifier = b.subscriberidentifier
AND a.insuranceplanidentifier = b.insuranceplanidentifier
WHERE (a.coverageStartDate >= b.coverageStartDate AND a.coverageStartDate <= b.coverageEndDate)
AND (a.coverageEndDate >= b.coverageStartDate AND a.coverageEndDate <= b.coverageEndDate)
;
--####################################################################################################################################
-- 8. Create final insured member profile "B" table - stage II
--####################################################################################################################################
CREATE TABLE IF NOT EXISTS wha.B_001_InsuredMemberProfile
STORED AS ORC tblproperties("orc.compress"="SNAPPY")
AS
SELECT
issuerkey AS issueridentifier, subscriberindicator, subscriberidentifier, insuranceplanidentifier, coveragestartdate, coverageenddate, enrollmentmaintenancetypecode, insuranceplanpremiumamount, rateareaidentifier AS exchangerateareaidentifier, hsaproductkey, hsariskpoolkey, riskadjustedindicator, hsalegalentitykey, hsarateareakey, hsalifestagekey, hsametallevelkey, grandfatheredplanindicator AS hsagrandfatheredplan, newmemberindicator AS hsanewmember, insurerstate AS hsaissuerstate, mbrstate AS hsamemberstate, hsamembergeokey, csr AS hsacsr, insuredmemberincome AS hsamemberincome, channel AS hsaonexchange, memberreckey, insuredmemberidentifier, groupidentifier, groupname, subgroupidentifier, subgroupname, contractnumber, relationship, membername, ageatenrollment, memberpremiumamount, 
CASE WHEN riskadjustedindicator = 1 AND ageatenrollment > 21 THEN 1 WHEN arf = 1 THEN 1 ELSE 0 END AS billablememberindicator, 
CASE WHEN memberpremiumamount = 0.0 AND subscriberIdentifier = insuredmemberidentifier AND riskadjustedindicator = 1 THEN 0 ELSE hsameaccepted END AS hsameaccepted, 
gcf
FROM
(
SELECT 
a.issuerkey, a.subscriberindicator, a.subscriberidentifier, a.coveragestartdate, a.coverageenddate, a.insuranceplanidentifier, a.enrollmentmaintenancetypecode, a.insuranceplanpremiumamount, a.rateareaidentifier, a.csr, a.insuredmemberincome, a.channel, a.insurerstate, a.mbrstate, a.grandfatheredplanindicator, a.newmemberindicator, a.hsaproductkey, a.hsariskpoolkey, a.riskadjustedindicator, a.hsalegalentitykey, a.hsarateareakey, a.hsalifestagekey, a.hsametallevelkey, a.hsamembergeokey, a.memberRecKey, a.insuredmemberidentifier, a.groupidentifier, a.groupname, a.subgroupidentifier, a.subgroupname, a.contractnumber, a.relationship, a.membername, a.ageatenrollment, a.billableMemberIndicator, a.GCF, b.arf,
CASE 
WHEN b.memberreckey IS NOT NULL 
AND b.insurancePlanIdentifier IS NOT NULL 
AND b.coveragestartdate IS NOT NULL 
AND b.coverageenddate IS NOT NULL 
THEN b.premium 
ELSE 0.00 
END AS memberpremiumamount,
CASE WHEN b.memberreckey IS NOT NULL AND b.insurancePlanIdentifier IS NOT NULL AND b.coveragestartdate IS NOT NULL AND b.coverageenddate IS NOT NULL THEN 1 ELSE 0 END AS hsameaccepted
FROM wha.insuredmemberprofile a
LEFT JOIN wha.arf b
ON a.memberRecKey = b.memberRecKey
AND a.insurancePlanIdentifier = b.insurancePlanIdentifier
AND a.coverageStartDate = b.coveragestartdate 
AND a.coverageEndDate = b.coverageEndDate
) AS X
;
--####################################################################################################################################
-- 9. Create planinfo "B" table
--####################################################################################################################################
CREATE TABLE IF NOT EXISTS wha.B_004_PlanInfo
STORED AS ORC tblproperties("orc.compress"="SNAPPY")
AS
SELECT
a.client, a.issueridentifier AS issuer, a.PlanRiskAdjustmentModel, a.issuerState, a.policyriskpool AS riskpool, a.policyproducttype AS producttype, a.policychannel AS channel, a.policymetallevel AS metallevel, a.policyproducttype AS networkType, m.PlanKey AS plankey, m.InsurancePlan AS insuranceplan, c.LegalEntityKey AS legalentitykey, d.IssuerKey AS issuerkey, COALESCE(j.StateKey, -1) AS statekey, COALESCE(h.PlanRiskAdjustmentModelKey, -1) AS planriskadjustmentmodelkey, COALESCE(g.RiskPoolKey, -1) AS riskpoolkey, COALESCE(e.ProductKey, -1) AS productkey, COALESCE(a.policychannel,0) AS channelKey, COALESCE(i.MetalLevelKey, -1) AS metallevelkey,COALESCE(e.ProductKey, -1) AS networkkey, a.policyRiskAdjustedIndicator AS riskadjustedindicator, a.policygrandfatheredIndicator AS grandfatheredplanindicator, FROM_UNIXTIME(UNIX_TIMESTAMP(), 'MM-dd-yyyy') AS dateoflastupdate
FROM wha.a_planinfo a
INNER JOIN reference.client b ON a.client = b.Client
INNER JOIN reference.legalentity c ON a.issuerlegalentity = c.legalentity
INNER JOIN reference.issuer d ON a.issueridentifier = d.Issuer
INNER JOIN reference.product e ON a.policyproducttype = e.product
LEFT JOIN reference.riskpool f ON a.policyriskpool = f.riskpool
LEFT JOIN reference.metallevel k ON a.policymetallevel = k.metallevelabbrev
LEFT JOIN  reference.state j ON a.issuerstate = j.state
LEFT JOIN reference.riskpool g ON a.policyriskpool = g.riskpool
LEFT JOIN reference.riskadjustmentmodel h ON a.planriskadjustmentmodel = h.planriskadjustmentmodelname
LEFT JOIN reference.metallevel i ON a.policymetallevel = i.metallevelabbrev
LEFT JOIN wha.plan m
ON 
COALESCE(b.clientkey, -1) = m.ClientKey 
AND COALESCE(c.LegalEntityKey, -1) = m.LegalEntityKey
AND COALESCE(d.IssuerKey, -1) = m.IssuerKey
AND COALESCE(a.insurancePlanIdentifier, -1) = m.InsurancePlan
AND COALESCE(e.ProductKey, -1) = m.ProductKey
AND COALESCE(f.RiskPoolKey, -1) = m.RiskPoolKey
AND COALESCE(k.MetalLevelKey, -1) = m.MetalLevelKey
AND COALESCE(j.StateKey, -1) = m.issuerStateKey
AND COALESCE(a.policychannel, -1) = m.channelKey
AND COALESCE(a.policyRiskAdjustedIndicator, -1) = m.RiskAdjustedIndicator
AND COALESCE(a.policygrandfatheredIndicator, -1) = m.grandfatheredPlanIndicator
;
INSERT OVERWRITE LOCAL DIRECTORY '/grid/0/nfs/user/sfox/output/WHA/B_001_InsuredMemberProfile' ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' SELECT * FROM wha.B_001_InsuredMemberProfile
;
INSERT OVERWRITE LOCAL DIRECTORY '/grid/0/nfs/user/sfox/output/WHA/B_004_PlanInfo' ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' SELECT * FROM wha.B_004_PlanInfo
;
INSERT OVERWRITE LOCAL DIRECTORY '/grid/0/nfs/user/sfox/output/WHA/B_000_InsuredMember' ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' SELECT * FROM wha.B_000_InsuredMember
;
--
-- END
--
