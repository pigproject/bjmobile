data = LOAD '$vlr_path' USING PigStorage(',');

data = FOREACH data  GENERATE (chararray) $0 AS REPORT_LENGTH, (chararray) $1 AS REPORT_TIME, (chararray) $2 AS REPORTS_SENT,  (chararray) $3 AS REPORT_NUMBER, (chararray) $4 AS VLR_UNIT_ID,  (long) $5 AS EVENT_TYPE, (chararray) $6 AS ATTEMPT_SUCCESS_INDICATOR,  (chararray) $7 AS ISDN_CATEGORY, (chararray) $8 AS IMSI,  (chararray) $9 AS IMEI_VALIDITY,  (chararray) $10 AS IMEI,  (chararray) $11 AS MS_CLASSMARK_VALIDITY, (chararray) $12 AS MS_CLASSMARK_CONTENT,  (chararray) $13 AS  MS_CLASSMARK_EXTENSION, (chararray) $14 AS MS_CLASSMARK_ENCRYPT_EXT, (chararray) $15 AS MSISDN_NUMBER_PLAN, (chararray) $16 AS MSISDN_NUMBER, (chararray) $17 AS LAC,  (chararray) $18 AS CELL,  (chararray) $19 AS RADIO_ACCESS_INFO,  (chararray) $20 AS DX_CAUSE,  (chararray) $21 AS SOURCE_ID, (chararray) $22 AS SAC,  (chararray) $23 AS SGSN_ATTACHED,  (chararray) $24 AS NBR_OF_IMSI_PAGINGS,  (chararray) $25 AS NBR_OF_TMSI_PAGINGS,  (chararray) $26 AS ANSWER_TIME,  (chararray) $27 AS ALTERNATE_CS_PAGING,  (chararray) $28 AS ROUTING_CATEGORY, (chararray) $29 AS ADD_ROUTING_CAT,  (chararray) $30 AS BSC_ID,  (chararray) $31 AS RNC_ID_MCC,  (chararray) $32 AS RNC_ID_MNC,  (chararray) $33 AS RNC_ID,  (chararray) $34 AS CELL_BAND,  (chararray) $35 AS LU_ERROR_CAUSE,  (chararray) $36 AS ROAMING_STATUS, (chararray) $37 AS PLMN_ID_NEW, (chararray) $38 AS PREVIOUS_MCC,  (chararray) $39 AS PREVIOUS_MNC,  (chararray) $40 AS PREVIOUS_LAC, (chararray) $41 AS PAGING_REASON, (chararray) $42 AS SUB_ID, (chararray) $43 AS IHSPA_INDICATOR, (chararray) $44 AS TRAF_PRIKEY_AUXCNTRCOL;

data = FILTER data BY EVENT_TYPE == 16777216;


grpd = GROUP data BY (MSISDN_NUMBER,REPORT_TIME);
--dump grpd;
grpd = FOREACH grpd {
        grop = ORDER data BY REPORT_TIME ;
        GENERATE group, grop ;
}

data = FOREACH grpd GENERATE FLATTEN(group) as (MSISDN_NUMBER,REPORT_TIME) ;

STORE data INTO '$output' USING PigStorage(',');
