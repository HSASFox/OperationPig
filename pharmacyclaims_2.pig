-- load raw pharmacy claims data using pig
pbc_pharmacyclaims1 = LOAD '/user/sfox/healthscape_pharmacy_file.txt' USING PigStorage  AS
(nationalDrugCode:chararray, genericMultiSingleBrand:chararray, rxTier:chararray, serviceFromDate:chararray, prescriptionFillDate:chararray, claimIdentifier:chararray, insuredMemberIdentifier:chararray, memberBirthDate:chararray, subscriberID:chararray, dispensingProviderIdentifier:chararray, serviceProviderNPI:chararray, totalBilledAmount:double, allowedtotalcostamount:double, policypaidamount:double, groupID:chararray, subgroupID: chararray)
;
-- make all column transformations 
pbc_pharmacyclaims2 = FOREACH pbc_pharmacyclaims1 GENERATE 
TRIM(insuredMemberIdentifier) AS insuredmemberidentifier, TRIM(claimIdentifier) AS claimidentifier, ToDate('1900/01/01','yyyy/MM/dd') AS claimprocesseddatetime, TRIM(prescriptionFillDate) AS prescriptionfilldate, ToDate('1900/01/01','yyyy/MM/dd') AS issuerclaimpaiddate, '' AS prescriptionservicereferencenumber, TRIM(nationalDrugCode) AS nationaldrugcode, '' AS dispensingprovideridqualifier, TRIM(dispensingProviderIdentifier) AS dispensingprovideridentifier, 0 AS prescriptionfillnumber, '' AS dispensingstatuscode, '' AS voidreplacecode, allowedtotalcostamount, policypaidamount, '' AS derivedserviceclaimindicator, TRIM(dispensingProviderIdentifier) AS prescribingprovideridentifier, '0' AS importmonthnumber, CONCAT((chararray)GetMonth(CurrentTime()),'-',(chararray)GetDay(CurrentTime()),'-',(chararray)GetYear(CurrentTime())) AS importdate, '0' AS iscurrent
;
pbc_pharmacyclaims3 = DISTINCT pbc_pharmacyclaims2
;
-- pharmacy claims have now been imported 
-- STEP 2: GET PHARMACY CLAIM AMOUNTS
pbc_pharmacyclaimsamount1 =  FOREACH pbc_pharmacyclaims2 GENERATE 
insuredmemberidentifier, claimidentifier, allowedtotalcostamount AS allowedtotalamount, policypaidamount AS policypaidtotalamount, prescriptionfilldate AS servicefromdate, prescriptionfilldate AS servicetodate, importmonthnumber, importdate, iscurrent
; 
-- store results to WHA_pbc_pharmacyclaims 
STORE pbc_pharmacyclaims3 INTO '/user/sfox/WHA_A_pharmacyclaims.txt' USING PigStorage('|','-schema')
;
-- store pharmacy claims amount out to 'WHA_pbc_pharmacyclaims_amt.txt'
STORE pbc_pharmacyclaimsamount1 INTO '/user/sfox/WHA_A_pharmacyclaims_amt.txt' USING PigStorage('|','-schema')
;
