--####################################################################################################################################################
-- WHA Member Info Data Migration Script
-- From: raw data To: "A" table format
-- Client: WHA
-- Note: to run from command line use: "pig -x tez /grid/0/nfs/user/sfox/Scripts/memberinfo_3.pig"
--####################################################################################################################################################
client_1 = LOAD '/user/sfox/Reference/Client.txt' USING PigStorage('\t') AS 
(client:chararray,clientkey:int)
;
geoarea_1 = LOAD '/user/sfox/Reference/GeoArea.txt' USING PigStorage('\t') AS
(geokey:int,zip:chararray,city:chararray,state:chararray,areacode:chararray,county:chararray,msa:chararray,pmsa:chararray,cbsa:chararray,cbsa_div:chararray,cbsatype:chararray,ratingareaidentifier:chararray)
;
referencegroup_1 = LOAD '/user/sfox/Reference/Group.txt' USING PigStorage('\t') AS
(groupid:chararray,groupkey:int)
;
issuer_1 = LOAD '/user/sfox/Reference/Issuer.txt' USING PigStorage('\t') AS
(issuer:chararray,issuerkey:int)
;
legalentity_1 = LOAD '/user/sfox/Reference/LegalEntity.txt' USING PigStorage('\t') AS
(legalentity:chararray,legalentitykey:int)
;
lifestage_1 = LOAD '/user/sfox/Reference/LifeStage.txt' USING PigStorage('\t') AS
(lifestage:chararray,lifestagekey:int)
;
metallevel_1 = LOAD '/user/sfox/Reference/MetalLevel.txt' USING PigStorage('\t') AS
(metallevelkey:int,metallevel:chararray,selectionfactor:double,selectionfactormin:double,selectionfactormax:double,selectionfactorrange:chararray,metallevelabbrev:chararray)
;
product_1 = LOAD '/user/sfox/Reference/Product.txt' USING PigStorage('\t') AS
(product:chararray,productkey:int)
;
ratearea_1 = LOAD '/user/sfox/Reference/RateArea.txt' USING PigStorage('\t') AS
(ratearea:chararray,rateareakey:int)
;
riskpool_1 = LOAD '/user/sfox/Reference/RiskPool.txt' USING PigStorage('\t') AS 
(riskpool:chararray, riskpoolkey:int)
;
riskadjustmentmodel_1 = LOAD '/user/sfox/Reference/RiskAdjustmentModel.txt' USING PigStorage('\t') AS
(planriskadjustmentmodelkey:int, planriskadjustmentmodelname:chararray)
;
state_1 = LOAD '/user/sfox/Reference/State.txt' USING PigStorage('\t') AS
(state:chararray,statekey:int)
;
pbc_memberinfo_1 = LOAD '/user/sfox/healthscape_member_file.txt' USING PigStorage ('\t') AS 
(insuredmemberidentifier:chararray,patientrelationshipcode:chararray,insuredmemberbirthdate:chararray,insuredmembergendercode:chararray,insuredmemberzipcode:chararray,coveragestartdate:chararray,coverageenddate:chararray,personindividualid:chararray,primarycareprovider:chararray,subscriberidentifier:chararray,subscriberdob:chararray,subscribergender:chararray,groupnumber:chararray,groupsize:chararray,policyproducttype:chararray,planid:chararray,plandesc: chararray,subpolicyeffdate:chararray,subpolicytermdate:chararray,insuranceplanidentifier:chararray,pharmacyplancode:chararray,policymetallevel:chararray,issuerlegalentity:chararray,policyriskadjustedindicator:chararray,insuredmemberlastname:chararray,insuredmemberfirstname:chararray,policyriskpool:chararray)
;
pbc_memberinfo_2 = FOREACH pbc_memberinfo_1 GENERATE 
$0 AS pkid, TRIM(insuredmemberidentifier) AS insuredmemberidentifier, TRIM(insuredmemberbirthdate) AS insuredmemberbirthdate, TRIM(insuredmembergendercode) AS insuredmembergendercode, (insuredmemberidentifier == subscriberidentifier?'S':'') AS subscriberindicator, TRIM(subscriberidentifier) AS subscriberidentifier, TRIM(insuranceplanidentifier) AS insuranceplanidentifier,(coveragestartdate < '01/01/2014' AND coverageenddate >= '01/01/2014'?'01/01/2014':coveragestartdate) AS coveragestartdate, TRIM(coverageenddate) AS coverageenddate, '' AS enrollmentmaintenancetypecode, 0.00 AS insuranceplanpremiumamount, '' AS rateareaidentifier, 'WHA' AS client, TRIM(issuerlegalentity) AS issuerlegalentity, 'CA' AS issuerstate, 'WHA: HMO' AS issueridentifier, TRIM(policyriskpool) AS policyriskpool, TRIM(policyproducttype) AS policyproducttype, TRIM(policymetallevel) AS policymetallevel, SUBSTRING(insuredmemberzipcode,0,5) AS insuredmemberzipcode, '' AS insuredmemberstate, '' AS policychannel, 'Test Life' AS insuredmemberlifestage, 60000 AS insuredmemberincome, '' AS policycsr, '' AS newmemberindicator, '' AS edgeserversubmissiondate, 0 AS policygrandfatheredindicator, (policyriskadjustedindicator=='N'?0:1) AS policyriskadjustedindicator, TRIM(groupnumber) AS groupnumber, TRIM(issuerlegalentity) AS groupname, '' AS subgroupidentifier, '' AS subgroupname, '' AS contractnumber, (patientrelationshipcode=='M'?1:(patientrelationshipcode=='S'?2:3)) AS insuredmemberrelationshipcode, CONCAT(insuredmemberfirstname,' ',insuredmemberlastname) AS membername, 0 AS importmonthnumber, CONCAT((chararray)GetMonth(CurrentTime()),'-',(chararray)GetDay(CurrentTime()),'-',(chararray)GetYear(CurrentTime())) AS importdate, 0 AS iscurrent
;
pbc_memberinfo_3 = JOIN pbc_memberinfo_2 BY LOWER(policyriskpool) LEFT, riskpool_1 BY LOWER(riskpool) USING 'skewed'
;
pbc_memberinfo_4 = JOIN pbc_memberinfo_3 BY LOWER(issuerlegalentity) LEFT, legalentity_1 BY LOWER(legalentity) USING 'skewed'
;
pbc_memberinfo_5 = JOIN pbc_memberinfo_4 BY LOWER(insuredmemberlifestage) LEFT, lifestage_1 BY LOWER(lifestage) USING 'skewed'
;
pbc_memberinfo_6 = JOIN pbc_memberinfo_5 BY LOWER(policymetallevel) LEFT, metallevel_1 BY LOWER(metallevelabbrev) USING 'skewed'
;
pbc_memberinfo_7 = JOIN pbc_memberinfo_6 BY LOWER(policyproducttype) LEFT, product_1 BY LOWER(product) USING 'skewed'
;
pbc_memberinfo_8 = JOIN pbc_memberinfo_7 BY LOWER(insuredmemberzipcode) LEFT, geoarea_1 BY LOWER(zip) USING 'skewed'
;
-- A table format - memberinfo
a_memberinfo_1 = FOREACH pbc_memberinfo_8 GENERATE 
pbc_memberinfo_2::insuredmemberidentifier AS insuredmemberidentifier,pbc_memberinfo_2::insuredmemberbirthdate AS insuredmemberbirthdate,pbc_memberinfo_2::insuredmembergendercode AS insuredmembergendercode,pbc_memberinfo_2::subscriberindicator AS subscriberindicator,pbc_memberinfo_2::subscriberidentifier AS subscriberidentifier,pbc_memberinfo_2::insuranceplanidentifier AS insuranceplanidentifier,pbc_memberinfo_2::coveragestartdate AS coveragestartdate,pbc_memberinfo_2::coverageenddate AS coverageenddate,pbc_memberinfo_2::enrollmentmaintenancetypecode AS enrollmentmaintenancetypecode,pbc_memberinfo_2::insuranceplanpremiumamount AS insuranceplanpremiumamount,pbc_memberinfo_2::rateareaidentifier AS rateareaidentifier,pbc_memberinfo_2::client AS client,pbc_memberinfo_2::issuerlegalentity AS legalentity,pbc_memberinfo_2::issuerstate AS issuerstate,pbc_memberinfo_2::issueridentifier AS issuer,pbc_memberinfo_2::policyriskpool AS policyriskpool,pbc_memberinfo_2::policyproducttype AS policyproduct,pbc_memberinfo_2::policymetallevel AS metallevel,pbc_memberinfo_2::insuredmemberzipcode AS mbrzipcode,pbc_memberinfo_2::insuredmemberstate AS mbrstate,pbc_memberinfo_2::policychannel AS channel,pbc_memberinfo_2::insuredmemberlifestage AS insuredmemberlifestage,pbc_memberinfo_2::insuredmemberincome AS insuredmemberincome,pbc_memberinfo_2::policycsr AS csr,pbc_memberinfo_2::newmemberindicator AS newmemberindicator,pbc_memberinfo_2::edgeserversubmissiondate AS edgeserversubmissiondate,pbc_memberinfo_2::policygrandfatheredindicator AS grandfatheredplanindicator,pbc_memberinfo_2::policyriskadjustedindicator AS riskadjustedindicator,pbc_memberinfo_2::groupnumber AS groupidentifier,pbc_memberinfo_2::groupname AS groupname,pbc_memberinfo_2::subgroupidentifier AS subgroupidentifier,pbc_memberinfo_2::subgroupname AS subgroupname,pbc_memberinfo_2::contractnumber AS contractnumber,pbc_memberinfo_2::insuredmemberrelationshipcode as relationship,pbc_memberinfo_2::membername AS membername,(product_1::productkey IS NULL?-1:product_1::productkey) AS hsaproductkey,(riskpool_1::riskpoolkey IS NULL?-1:riskpool_1::riskpoolkey) AS hsariskpoolkey,(legalentity_1::legalentitykey IS NULL?-1:legalentity_1::legalentitykey) AS hsalegalentitykey,1 AS hsarateareakey,(legalentity_1::legalentitykey IS NULL?-1:lifestage_1::lifestagekey) AS hsalifestagekey,(metallevel_1::metallevelkey IS NULL?-1:metallevel_1::metallevelkey) AS hsametallevelkey,(geoarea_1::geokey IS NULL?-1:geoarea_1::geokey) AS hsamembergeokey,pbc_memberinfo_2::importmonthnumber AS importmonthnumber,pbc_memberinfo_2::importdate AS importdate,pbc_memberinfo_2::iscurrent AS iscurrent
;
pbc_planinfo_1 = FOREACH pbc_memberinfo_2 GENERATE
client, issuerlegalentity, issuerstate, insuranceplanidentifier, issueridentifier, 'Federal HHS-HCC' AS planriskadjustmentmodel, policyriskpool, policyproducttype, policychannel, policymetallevel, policygrandfatheredindicator, policyriskadjustedindicator, policycsr
;
-- A table format - plan info
a_planinfo_1 = DISTINCT pbc_planinfo_1
;
------------------------------------------------------------------------------------------------------------------------------------------------------
-- Store final data sets in HDFS to be sqooped to SQL Server, pipe ('|') delimited
------------------------------------------------------------------------------------------------------------------------------------------------------
STORE a_memberinfo_1 INTO '/user/sfox/WHA_A_memberinfo.txt' USING PigStorage('|','-schema');
STORE a_planinfo_1 INTO '/user/sfox/WHA_A_planinfo.txt' USING PigStorage('|','-schema');
