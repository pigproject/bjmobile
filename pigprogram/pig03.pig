register 'bjmobile.jar'

define ReportTime com.bjmobile.ReportTime();
define ReportTimeGroup com.bjmobile.ReportTimeGroup();
define ReportInterval  com.bjmobile.ReportInterval();

data = LOAD '$vlr_path' USING PigStorage(',');

data = FOREACH data  GENERATE (chararray) $0 AS REPORT_LENGTH, (chararray) $1 AS REPORT_TIME, (chararray) $2 AS REPORTS_SENT,  (chararray) $3 AS REPORT_NUMBER, (chararray) $4 AS VLR_UNIT_ID,  (long) $5 AS EVENT_TYPE, (chararray) $6 AS ATTEMPT_SUCCESS_INDICATOR,  (chararray) $7 AS ISDN_CATEGORY, (chararray) $8 AS IMSI,  (chararray) $9 AS IMEI_VALIDITY,  (chararray) $10 AS IMEI,  (chararray) $11 AS MS_CLASSMARK_VALIDITY, (chararray) $12 AS MS_CLASSMARK_CONTENT,  (chararray) $13 AS  MS_CLASSMARK_EXTENSION, (chararray) $14 AS MS_CLASSMARK_ENCRYPT_EXT, (chararray) $15 AS MSISDN_NUMBER_PLAN, (chararray) $16 AS MSISDN_NUMBER, (chararray) $17 AS LAC,  (int) $18 AS CELL,  (chararray) $19 AS RADIO_ACCESS_INFO,  (chararray) $20 AS DX_CAUSE,  (chararray) $21 AS SOURCE_ID, (chararray) $22 AS SAC,  (chararray) $23 AS SGSN_ATTACHED,  (chararray) $24 AS NBR_OF_IMSI_PAGINGS,  (chararray) $25 AS NBR_OF_TMSI_PAGINGS,  (chararray) $26 AS ANSWER_TIME,  (chararray) $27 AS ALTERNATE_CS_PAGING,  (chararray) $28 AS ROUTING_CATEGORY, (chararray) $29 AS ADD_ROUTING_CAT,  (chararray) $30 AS BSC_ID,  (chararray) $31 AS RNC_ID_MCC,  (chararray) $32 AS RNC_ID_MNC,  (chararray) $33 AS RNC_ID,  (chararray) $34 AS CELL_BAND,  (chararray) $35 AS LU_ERROR_CAUSE,  (chararray) $36 AS ROAMING_STATUS, (chararray) $37 AS PLMN_ID_NEW, (chararray) $38 AS PREVIOUS_MCC,  (chararray) $39 AS PREVIOUS_MNC,  (chararray) $40 AS PREVIOUS_LAC, (chararray) $41 AS PAGING_REASON, (chararray) $42 AS SUB_ID, (chararray) $43 AS IHSPA_INDICATOR, (chararray) $44 AS TRAF_PRIKEY_AUXCNTRCOL;

rtt_data = LOAD '$rtt_path' USING PigStorage(',');
rtt_data = FOREACH rtt_data GENERATE (chararray) $0 as NEW_SUB_ID, (chararray) $1 as RTT_REPORT_TIME, (chararray) $2 as REPORT_COUNT, (chararray) $3 as CALL_AMOUNT, (chararray) $4 as CALL_START_TIME, (chararray) $5 as SIGNALLING_COMPLETE_TIME, (chararray) $6 as B_ANSWERED_TIME, (chararray) $7 as CHARGING_END_TIME, (chararray) $8 as A_IMSI, (chararray) $9 as A_IMEI, (chararray) $10 as A_DIRECTION_NUMBER_TYPE, (chararray) $11 as A_DIRECTION_NUMBER, (chararray) $12 as A_PNP_NUMBER, (chararray) $13 as A_PRIORITY, (chararray) $14 as A_CATEGORY, (chararray) $15 as A_CLASSMARK_CONTENT, (chararray) $16 as A_CLASSMARK_EXTENSION, (chararray) $17 as A_CLASSMARK_ENCRYPT_EXT, (chararray) $18 as A_LAC, (chararray) $19 as A_CELL, (chararray) $20 as A_FIRST_LAC, (chararray) $21 as A_FIRST_CELL, (chararray) $22 as A_BSC, (chararray) $23 as A_CHANNEL_RATE_PROPOSED, (chararray) $24 as A_CHANNEL_RATE_USED, (chararray) $25 as A_PCM, (chararray) $26 as A_TSL, (chararray) $27 as A_CGR, (chararray) $28 as A_TELE_SERVICE, (chararray) $29 as A_BEARER_SERVICE, (chararray) $30 as A_USED_SERVICES_1, (chararray) $31 as A_USED_SERVICES_2, (chararray) $32 as A_USED_SERVICES_3, (chararray) $33 as A_USED_SERVICES_4, (chararray) $34 as A_CUG_INTERLOCK_INFO, (chararray) $35 as A_CUG_INTERLOCK_BCODE, (chararray) $36 as A_CUG_INDEX, (chararray) $37 as A_CUG_OUTGOING_ACCESS, (chararray) $38 as A_SPEECH_VERSION, (chararray) $39 as A_CELL_BAND, (chararray) $40 as A_DUAL_BAND, (chararray) $41 as B_IMSI, (chararray) $42 as B_IMEI, (chararray) $43 as B_DIRECTION_NUMBER_TYPE, (chararray) $44 as B_DIRECTION_NUMBER, (chararray) $45 as B_PNP_NUMBER, (chararray) $46 as B_PRIORITY, (chararray) $47 as B_CATEGORY, (chararray) $48 as B_CLASSMARK_CONTENT, (chararray) $49 as B_CLASSMARK_EXTENSION, (chararray) $50 as B_CLASSMARK_ENCRYPT_EXT, (chararray) $51 as B_LAC, (chararray) $52 as B_CELL, (chararray) $53 as B_FIRST_LAC, (chararray) $54 as B_FIRST_CELL, (chararray) $55 as B_BSC, (chararray) $56 as B_CHANNEL_RATE_PROPOSED, (chararray) $57 as B_CHANNEL_RATE_USED, (chararray) $58 as B_PCM, (chararray) $59 as B_TSL, (chararray) $60 as B_CGR, (chararray) $61 as B_TELE_SERVICE, (chararray) $62 as B_BEARER_SERVICE, (chararray) $63 as B_CUG_INTERLOCK_INFO, (chararray) $64 as B_CUG_INTERLOCK_BCODE, (chararray) $65 as B_CUG_INDEX, (chararray) $66 as B_CUG_OUTGOING_ACCESS, (chararray) $67 as B_SPEECH_VERSION, (chararray) $68 as B_CELL_BAND, (chararray) $69 as B_DUAL_BAND, (chararray) $70 as A_DATA_PCM, (chararray) $71 as A_DATA_TSL, (chararray) $72 as A_DATA_CGR, (chararray) $73 as A_DATA_CLEAR_CODE, (chararray) $74 as A_DATA_BCIE_GSM_BC_BASIC_INFO, (chararray) $75 as A_DATA_BCIE_OCTET_4, (chararray) $76 as A_DATA_BCIE_OCTET_5, (chararray) $77 as A_DATA_BCIE_OCTET_6, (chararray) $78 as A_DATA_BCIE_OCTET_6A, (chararray) $79 as A_DATA_BCIE_OCTET_6B, (chararray) $80 as A_DATA_BCIE_OCTET_6C, (chararray) $81 as A_DATA_BCIE_OCTET_6D, (chararray) $82 as A_DATA_BCIE_OCTET_6E, (chararray) $83 as A_DATA_BCIE_OCTET_6F, (chararray) $84 as A_DATA_BCIE_OCTET_7, (chararray) $85 as A_DATA_BCIE_SPARE, (chararray) $86 as B_DATA_PCM, (chararray) $87 as B_DATA_TSL, (chararray) $88 as B_DATA_CGR, (chararray) $89 as B_DATA_CLEAR_CODE, (chararray) $90 as EMERGENCY_CALL, (chararray) $91 as DX_CAUSE, (chararray) $92 as CLEAR_ADD_INFO_SET_PRB, (chararray) $93 as CLEAR_ADD_INFO_INT_SITUATION, (chararray) $94 as CLEAR_ADD_INFO_INTERNAL_USE, (chararray) $95 as RELEASE_PART, (chararray) $96 as FORW_COUNT, (chararray) $97 as EXT_FORW_COUNT, (chararray) $98 as PAGING_TIME, (chararray) $99 as REDIRECTOR_DIRECTION_NUMBER_TYPE, (chararray) $100 as REDIRECTOR_DIRECTION_NUMBER, (chararray) $101 as B_MSRN_NUMBER_TYPE, (chararray) $102 as B_MSRN_NUMBER, (chararray) $103 as OUT_PULSED_NUMBER_TYPE, (chararray) $104 as OUT_PULSED_NUMBER, (chararray) $105 as FORWARDED_TO_NUMBER_TYPE, (chararray) $106 as FORWARDED_TO_NUMBER, (chararray) $107 as MF_CIRCUIT_PCM, (chararray) $108 as MF_CIRCUIT_TSL, (chararray) $109 as PBRU_CIRCUIT_PCM, (chararray) $110 as PBRU_CIRCUIT_TSL, (chararray) $111 as CNFC_CIRCUIT_PCM, (chararray) $112 as CNFC_CIRCUIT_TSL, (chararray) $113 as USED_TNS_NUMBER_CIC, (chararray) $114 as CARRIER_SELECTION, (chararray) $115 as LAST_SCP_GT, (chararray) $116 as LAST_SCP_CALL_GAPPED, (chararray) $117 as LAST_SCP_CNT_DIALOG, (chararray) $118 as LAST_SCP_CNT_TYPE, (chararray) $119 as LAST_BCSM, (chararray) $120 as A_BSSMAP_CAUSE_LENGTH, (chararray) $121 as A_BSSMAP_CAUSE, (chararray) $122 as A_BSSMAP_CAUSE_EXT, (chararray) $123 as B_BSSMAP_CAUSE_LENGTH, (chararray) $124 as B_BSSMAP_CAUSE, (chararray) $125 as B_BSSMAP_CAUSE_EXT, (chararray) $126 as A_FIRST_BAND, (chararray) $127 as B_FIRST_BAND, (chararray) $128 as SOURCE_ID, (chararray) $129 as A_USED_CHANNELS, (chararray) $130 as B_USED_CHANNELS, (chararray) $131 as A_EXT_CLEAR_CGR, (chararray) $132 as A_USED_FR_1_CHANNEL_RATE_TIME, (chararray) $133 as A_USED_FR_2_CHANNEL_RATE_TIME, (chararray) $134 as A_USED_FR_3_CHANNEL_RATE_TIME, (chararray) $135 as A_USED_FR_4_CHANNEL_RATE_TIME, (chararray) $136 as B_USED_FR_1_CHANNEL_RATE_TIME, (chararray) $137 as B_USED_FR_2_CHANNEL_RATE_TIME, (chararray) $138 as B_USED_FR_3_CHANNEL_RATE_TIME, (chararray) $139 as B_USED_FR_4_CHANNEL_RATE_TIME, (chararray) $140 as IWF_RX_RLP_RETR_REQ, (chararray) $141 as IWF_RX_RLP_ALL_FRAMES, (chararray) $142 as IWF_TX_RLP_RETR, (chararray) $143 as IWF_TX_RLP_ALL_FRAMES, (chararray) $144 as IWF_CONNECTION_MODE, (chararray) $145 as IWF_GSM_V110_RESYNCS, (chararray) $146 as IWF_ITU_V110_RESYNCS, (chararray) $147 as IWF_RX_V120_RETR_REQ, (chararray) $148 as IWF_RX_V120_ALL_FRAMES, (chararray) $149 as IWF_TX_V120_RETR, (chararray) $150 as IWF_TX_V120_ALL_FRAMES, (chararray) $151 as IWF_ITU_V120_RESTARTS, (chararray) $152 as IWF_USED_RA_PROTOCOL, (chararray) $153 as IWF_USED_ITC, (chararray) $154 as IWF_ASYMM_CONF_INFO, (chararray) $155 as IWF_SPARE, (chararray) $156 as IWF_ATRAU_RESYNCS, (chararray) $157 as IWF_RX_COMPRESSION_STATUS, (chararray) $158 as IWF_TX_COMPRESSION_STATUS, (chararray) $159 as IWF_RX_V120_DC_STATUS, (chararray) $160 as IWF_TX_V120_DC_STATUS, (chararray) $161 as IWF_RX_DATA_IN_KBYTES, (chararray) $162 as IWF_RX_CDATA_IN_KBYTES, (chararray) $163 as IWF_TX_DATA_IN_KBYTES, (chararray) $164 as IWF_TX_CDATA_IN_KBYTES, (chararray) $165 as DEVICE_ID, (chararray) $166 as SERVICE_NUMBER, (chararray) $167 as A_PRIORITY_USAGE, (chararray) $168 as A_CLASSMARK_3_INFO2, (chararray) $169 as A_CLASSMARK_3_INFO3, (chararray) $170 as A_MSC_INT_CELL, (chararray) $171 as A_FIRST_MSC_INT_CELL, (chararray) $172 as A_LOCATION_NUMBER_TYPE, (chararray) $173 as A_LOCATION_NUMBER, (chararray) $174 as B_LOCATION_NUMBER_TYPE, (chararray) $175 as B_LOCATION_NUMBER, (chararray) $176 as A_DATA_CNTL_UNIT_TYPE, (chararray) $177 as A_DATA_CNTL_UNIT_INDEX, (chararray) $178 as A_DATA_BCIE_LENGTH_USED, (chararray) $179 as A_DATA_BCIE_GSM_BC_BASIC_INFO_USED, (chararray) $180 as A_DATA_BCIE_OCTET_4_USED, (chararray) $181 as A_DATA_BCIE_OCTET_5_USED, (chararray) $182 as A_DATA_BCIE_OCTET_5A_USED, (chararray) $183 as A_DATA_BCIE_OCTET_5B_USED, (chararray) $184 as A_DATA_BCIE_OCTET_6_USED, (chararray) $185 as A_DATA_BCIE_OCTET_6A_USED, (chararray) $186 as A_DATA_BCIE_OCTET_6B_USED, (chararray) $187 as A_DATA_BCIE_OCTET_6C_USED, (chararray) $188 as A_DATA_BCIE_OCTET_6D_USED, (chararray) $189 as A_DATA_BCIE_OCTET_6E_USED, (chararray) $190 as A_DATA_BCIE_OCTET_6F_USED, (chararray) $191 as A_DATA_BCIE_OCTET_7_USED, (chararray) $192 as B_DATA_CNTL_UNIT_TYPE, (chararray) $193 as B_DATA_CNTL_UNIT_INDEX, (chararray) $194 as A_SIGN_PROTOCOL, (chararray) $195 as A_EXT_CAUSE, (chararray) $196 as B_SIGN_PROTOCOL, (chararray) $197 as B_EXT_CAUSE, (chararray) $198 as A_LATEST_HO_TYPE, (chararray) $199 as B_LATEST_HO_TYPE, (chararray) $200 as A_ROUTE_CASE, (chararray) $201 as B_ROUTE_CASE, (chararray) $202 as INTER_MSC_HO, (chararray) $203 as ROAMING_INFO, (chararray) $204 as USED_CIP_NUMBER_CIC, (chararray) $205 as LEG_CG_PIC_NUMBER_CIC, (chararray) $206 as IN_CATEGORY_KEY, (chararray) $207 as LSA_ID_EXIST, (chararray) $208 as LSA_ID, (chararray) $209 as A_CHANNEL_TYPE_SPEECH_OR_DATA_IND, (chararray) $210 as A_CHANNEL_TYPE_SPEECH_ENCODE_ALGORITHM, (chararray) $211 as A_CHANNEL_TYPE_EXT_SPEECH_ENC_ALG, (chararray) $212 as A_AIF_IR, (chararray) $213 as LAST_SCP_CHOICE, (chararray) $214 as LAST_SCP_SERVICE_KEY, (chararray) $215 as LAST_SCP_CALLED_ADDRESS, (chararray) $216 as LAST_SCP_IN_TYPE_OF_CUSM, (chararray) $217 as LAST_SCP_AP_PROTOCOL, (chararray) $218 as B_EXT_CLEAR_CGR, (chararray) $219 as B_PRIORITY_USAGE, (chararray) $220 as B_CLASSMARK_3_INFO2, (chararray) $221 as B_CLASSMARK_3_INFO3, (chararray) $222 as B_MSC_INT_CELL, (chararray) $223 as B_FIRST_MSC_INT_CELL, (chararray) $224 as B_DATA_BCIE_LENGTH_USED, (chararray) $225 as B_DATA_BCIE_GSM_BC_BASIC_INFO_USED, (chararray) $226 as B_DATA_BCIE_OCTET_4_USED, (chararray) $227 as B_DATA_BCIE_OCTET_5_USED, (chararray) $228 as B_DATA_BCIE_OCTET_5A_USED, (chararray) $229 as B_DATA_BCIE_OCTET_5B_USED, (chararray) $230 as B_DATA_BCIE_OCTET_6_USED, (chararray) $231 as B_DATA_BCIE_OCTET_6A_USED, (chararray) $232 as B_DATA_BCIE_OCTET_6B_USED, (chararray) $233 as B_DATA_BCIE_OCTET_6C_USED, (chararray) $234 as B_DATA_BCIE_OCTET_6D_USED, (chararray) $235 as B_DATA_BCIE_OCTET_6E_USED, (chararray) $236 as B_DATA_BCIE_OCTET_6F_USED, (chararray) $237 as B_DATA_BCIE_OCTET_7_USED, (chararray) $238 as PAGING_THROUGH_SGSN, (chararray) $239 as B_CHANNEL_TYPE_SPEECH_OR_DATA_IND, (chararray) $240 as B_CHANNEL_TYPE_SPEECH_ENCODE_ALGORITHM, (chararray) $241 as B_CHANNEL_TYPE_EXT_SPEECH_ENC_ALG, (chararray) $242 as B_AIF_IR, (chararray) $243 as A_CAMEL_CALL_REFERENCE_MSC_ADDRESS_NUMBER, (chararray) $244 as A_CAMEL_CALL_REFERENCE_NUMBER, (chararray) $245 as B_CAMEL_CALL_REFERENCE_MSC_ADDRESS_NUMBER, (chararray) $246 as B_CAMEL_CALL_REFERENCE_NUMBER, (chararray) $247 as A_FIRST_BSC, (chararray) $248 as B_FIRST_BSC, (chararray) $249 as CALLED_SUBS_USED_SERVICES_1, (chararray) $250 as CALLED_SUBS_USED_SERVICES_3, (chararray) $251 as CALLED_SUBS_USED_SERVICES_2, (chararray) $252 as CALLED_SUBS_USED_SERVICES_4, (chararray) $253 as B_LAST_SCP_GT, (chararray) $254 as B_LAST_SCP_CALL_GAPPED, (chararray) $255 as B_LAST_SCP_CNT_DIALOG, (chararray) $256 as B_LAST_SCP_CHOICE, (chararray) $257 as B_LAST_SCP_SERVICE_KEY, (chararray) $258 as B_LAST_SCP_CALLED_ADDRESS, (chararray) $259 as B_LAST_SCP_CNT_TYPE, (chararray) $260 as B_LAST_SCP_IN_TYPE_OF_BCSM, (chararray) $261 as B_LAST_SCP_IN_TYPE_OF_CUSM, (chararray) $262 as B_LAST_SCP_AP_PROTOCOL, (chararray) $263 as A_CLASSMARK_3_INFO4, (chararray) $264 as A_CLASSMARK_3_INFO5, (chararray) $265 as A_MCC, (chararray) $266 as A_MNC, (chararray) $267 as A_FIRST_MCC, (chararray) $268 as A_FIRST_MNC, (chararray) $269 as A_DX_CAUSE, (chararray) $270 as A_RECEIVED_IEC_INDICATOR, (chararray) $271 as A_RECEIVED_OEC_INDICATOR, (chararray) $272 as A_IEC_TO_SEND_INDICATOR, (chararray) $273 as A_OEC_TO_SEND_INDICATOR, (chararray) $274 as A_IEC_STATE, (chararray) $275 as A_IEC_EXISTENCE, (chararray) $276 as A_OEC_STATE, (chararray) $277 as A_OEC_EXISTENCE, (chararray) $278 as A_TRANSPARENCY_INDICATION, (chararray) $279 as A_CAMEL_MODIFICATIONS, (chararray) $280 as A_FIRST_RNC, (chararray) $281 as A_RNC, (chararray) $282 as A_FIRST_ORIG_ACCESS_INFO, (chararray) $283 as A_FIRST_TCH_ACCESS_INFO, (chararray) $284 as A_TCH_ACCESS_INFO, (chararray) $285 as A_MEDIA_IP_ADDR_LOCAL_VER, (chararray) $286 as A_MEDIA_IP_ADDR_LOCAL, (chararray) $287 as A_MEDIA_PORT_NO_LOCAL, (chararray) $288 as A_MEDIA_IP_ADDR_REMOTE_VER, (chararray) $289 as A_MEDIA_IP_ADDR_REMOTE, (chararray) $290 as A_MEDIA_PORT_NO_REMOTE, (chararray) $291 as A_SIGN_IP_ADDR_LOCAL_VER, (chararray) $292 as A_SIGN_IP_ADDR_LOCAL, (chararray) $293 as A_SIGN_PORT_NO_LOCAL, (chararray) $294 as A_SIGN_IP_ADDR_REMOTE_VER, (chararray) $295 as A_SIGN_IP_ADDR_REMOTE, (chararray) $296 as A_SIGN_PORT_NO_REMOTE, (chararray) $297 as A_USED_USER_RATE, (chararray) $298 as B_CLASSMARK_3_INFO4, (chararray) $299 as B_CLASSMARK_3_INFO5, (chararray) $300 as B_MCC, (chararray) $301 as B_MNC, (chararray) $302 as B_FIRST_MCC, (chararray) $303 as B_FIRST_MNC, (chararray) $304 as B_DX_CAUSE, (chararray) $305 as B_RECEIVED_IEC_INDICATOR, (chararray) $306 as B_RECEIVED_OEC_INDICATOR, (chararray) $307 as B_IEC_TO_SEND_INDICATOR, (chararray) $308 as B_OEC_TO_SEND_INDICATOR, (chararray) $309 as B_IEC_STATE, (chararray) $310 as B_IEC_EXISTENCE, (chararray) $311 as B_OEC_STATE, (chararray) $312 as B_OEC_EXISTENCE, (chararray) $313 as B_TRANSPARENCY_INDICATION, (chararray) $314 as B_CAMEL_MODIFICATIONS, (chararray) $315 as B_FIRST_RNC, (chararray) $316 as B_RNC, (chararray) $317 as B_FIRST_ORIG_ACCESS_INFO, (chararray) $318 as B_FIRST_TCH_ACCESS_INFO, (chararray) $319 as B_TCH_ACCESS_INFO, (chararray) $320 as B_MEDIA_IP_ADDR_LOCAL_VER, (chararray) $321 as B_MEDIA_IP_ADDR_LOCAL, (chararray) $322 as B_MEDIA_PORT_NO_LOCAL, (chararray) $323 as B_MEDIA_IP_ADDR_REMOTE_VER, (chararray) $324 as B_MEDIA_IP_ADDR_REMOTE, (chararray) $325 as B_MEDIA_PORT_NO_REMOTE, (chararray) $326 as B_SIGN_IP_ADDR_LOCAL_VER, (chararray) $327 as B_SIGN_IP_ADDR_LOCAL, (chararray) $328 as B_SIGN_PORT_NO_LOCAL, (chararray) $329 as B_SIGN_IP_ADDR_REMOTE_VER, (chararray) $330 as B_SIGN_IP_ADDR_REMOTE, (chararray) $331 as B_SIGN_PORT_NO_REMOTE, (chararray) $332 as B_USED_USER_RATE, (chararray) $333 as B_IWF_RX_RLP_RETR_REQ, (chararray) $334 as B_IWF_RX_RLP_ALL_FRAMES, (chararray) $335 as B_IWF_TX_RLP_RETR, (chararray) $336 as B_IWF_TX_RLP_ALL_FRAMES, (chararray) $337 as B_IWF_CONNECTION_MODE, (chararray) $338 as B_IWF_GSM_V110_RESYNCS, (chararray) $339 as B_IWF_ITU_V110_RESYNCS, (chararray) $340 as B_IWF_LINE_QUALITY_HI, (chararray) $341 as B_IWF_LINE_QUALITY_MEAN, (chararray) $342 as B_IWF_LINE_QUALITY_LO, (chararray) $343 as B_IWF_DISCONNECT_REASON, (chararray) $344 as B_IWF_RETRAIN_REQ, (chararray) $345 as B_IWF_RETRAIN_ACK, (chararray) $346 as B_IWF_RATE_CHANGE_REQ, (chararray) $347 as B_IWF_RATE_CHANGE_ACK, (chararray) $348 as B_IWF_RX_V120_RETR_REQ, (chararray) $349 as B_IWF_RX_V120_ALL_FRAMES, (chararray) $350 as B_IWF_TX_V120_RETR, (chararray) $351 as B_IWF_TX_V120_ALL_FRAMES, (chararray) $352 as B_IWF_ITU_V120_RESTARTS, (chararray) $353 as B_IWF_USED_RA_PROTOCOL, (chararray) $354 as B_IWF_USED_ITC, (chararray) $355 as B_IWF_ASYMM_CONF_INFO, (chararray) $356 as B_IWF_SPARE, (chararray) $357 as B_IWF_ATRAU_RESYNCS, (chararray) $358 as B_IWF_RX_COMPRESSION_STATUS, (chararray) $359 as B_IWF_TX_COMPRESSION_STATUS, (chararray) $360 as B_IWF_RX_V120_DC_STATUS, (chararray) $361 as B_IWF_TX_V120_DC_STATUS, (chararray) $362 as B_IWF_RX_DATA_IN_KBYTES, (chararray) $363 as B_IWF_RX_CDATA_IN_KBYTES, (chararray) $364 as B_IWF_TX_DATA_IN_KBYTES, (chararray) $365 as B_IWF_TX_CDATA_IN_KBYTES, (chararray) $366 as A_STREAM_IDENTIFIER, (chararray) $367 as A_NBR_SB, (chararray) $368 as A_NBR_USER, (chararray) $369 as B_STREAM_IDENTIFIER, (chararray) $370 as B_NBR_SB, (chararray) $371 as B_NBR_USER, (chararray) $372 as REPORT_INTERNAL_INFO, (chararray) $373 as A_SAC, (chararray) $374 as A_MGW_NAME_LENGTH, (chararray) $375 as A_MGW_NAME, (chararray) $376 as A_FIRST_SAC, (chararray) $377 as B_SAC, (chararray) $378 as B_MGW_NAME_LENGTH, (chararray) $379 as B_MGW_NAME, (chararray) $380 as B_FIRST_SAC, (chararray) $381 as REPORT_LENGTH, (chararray) $382 as A_NUMBER_OF_HANDOVERS, (chararray) $383 as B_NUMBER_OF_HANDOVERS, (chararray) $384 as A_UPD_INDEX, (chararray) $385 as A_UPD_NAME_LENGTH, (chararray) $386 as A_UPD_NAME, (chararray) $387 as A_BNC_CHAR, (chararray) $388 as B_UPD_INDEX, (chararray) $389 as B_UPD_NAME_LENGTH, (chararray) $390 as B_UPD_NAME, (chararray) $391 as B_BNC_CHAR, (chararray) $392 as EMERGENCY_CATEGORY, (chararray) $393 as B_ADDRESS_NUMBER_TYPE, (chararray) $394 as B_ADDRESS_NUMBER, (chararray) $395 as B_USED_SERVICES_1, (chararray) $396 as B_USED_SERVICES_2, (chararray) $397 as B_USED_SERVICES_3, (chararray) $398 as B_USED_SERVICES_4, (chararray) $399 as A_CONTROLLING_IWF_ADDRESS_MGW_VERSION, (chararray) $400 as A_CONTROLLING_IWF_ADDRESS_MGW_ADDR, (chararray) $401 as A_CONTROLLING_IWF_ADDRESS_IWF_VERSION, (chararray) $402 as A_CONTROLLING_IWF_ADDRESS_IWF_ADDR, (chararray) $403 as A_CONTROLLING_IWF_H248_MGW_PORT, (chararray) $404 as A_CONTROLLING_IWF_NUMBER_ADDRESS_LENGTH, (chararray) $405 as A_CONTROLLING_IWF_INDICATORS, (chararray) $406 as A_CONTROLLING_IWF_NUMBERS, (chararray) $407 as A_CONTROLLING_IW_MGW_ID, (chararray) $408 as A_CONTROLLING_IW_DATA_CIRCUIT_PCM, (chararray) $409 as A_CONTROLLING_IW_DATA_CIRCUIT_TSL, (chararray) $410 as B_CONTROLLING_IWF_ADDRESS_MGW_VERSION, (chararray) $411 as B_CONTROLLING_IWF_ADDRESS_MGW_ADDR, (chararray) $412 as B_CONTROLLING_IWF_ADDRESS_IWF_VERSION, (chararray) $413 as B_CONTROLLING_IWF_ADDRESS_IWF_ADDR, (chararray) $414 as B_CONTROLLING_IWF_H248_MGW_PORT, (chararray) $415 as B_CONTROLLING_IWF_NUMBER_ADDRESS_LENGTH, (chararray) $416 as B_CONTROLLING_IWF_INDICATORS, (chararray) $417 as B_CONTROLLING_IWF_NUMBERS, (chararray) $418 as B_CONTROLLING_IW_MGW_ID, (chararray) $419 as B_CONTROLLING_IW_DATA_CIRCUIT_PCM, (chararray) $420 as B_CONTROLLING_IW_DATA_CIRCUIT_TSL, (chararray) $421 as A_INTERNAL_TERMINATION_ID_INDEX, (chararray) $422 as A_INTERNAL_TERMINATION_ID_FORMAT, (chararray) $423 as A_INTERNAL_TERMINATION_ID_TYPE, (chararray) $424 as A_INTERNAL_TERMINATION_ID_UNION, (chararray) $425 as B_INTERNAL_TERMINATION_ID_INDEX, (chararray) $426 as B_INTERNAL_TERMINATION_ID_FORMAT, (chararray) $427 as B_INTERNAL_TERMINATION_ID_TYPE, (chararray) $428 as B_INTERNAL_TERMINATION_ID_UNION, (chararray) $429 as ORIG_CALL_ID, (chararray) $430 as A_ROUTING_CATEGORY, (chararray) $431 as A_ADD_ROUTING_CAT, (chararray) $432 as B_ROUTING_CATEGORY, (chararray) $433 as B_ADD_ROUTING_CAT, (chararray) $434 as FORWARDED_TO_ROUTE_CASE, (chararray) $435 as FORWARDING_SIGNALLING_COMPLETE_TIME, (chararray) $436 as A_FIRST_INTERNAL_TERMINATION_ID_INDEX, (chararray) $437 as A_FIRST_INTERNAL_TERMINATION_ID_FORMAT, (chararray) $438 as A_FIRST_INTERNAL_TERMINATION_ID_TYPE, (chararray) $439 as A_FIRST_INTERNAL_TERMINATION_ID_UNION, (chararray) $440 as A_FIRST_UPD_INDEX, (chararray) $441 as REPORTING_COMPUTER_UNIT_ID_TYPE, (chararray) $442 as REPORTING_COMPUTER_UNIT_ID_INDEX, (chararray) $443 as CMN_MODE, (chararray) $444 as FIRST_CMN_MODE, (chararray) $445 as PART_TIME_TRFO, (chararray) $446 as ALL_TIME_TRFO, (chararray) $447 as TDMC_OUT_QUASI_CRCT_PCM, (chararray) $448 as TDMC_OUT_QUASI_CRCT_TSL, (chararray) $449 as TDMC_OUT_TERMID_INDEX, (chararray) $450 as TDMC_OUT_TERMID_FORMAT, (chararray) $451 as TDMC_OUT_TERMID_TYPE, (chararray) $452 as TDMC_OUT_TERMID_UNION, (chararray) $453 as TDMC_OUT_MGW_NAME_LENGTH, (chararray) $454 as TDMC_OUT_MGW_NAME, (chararray) $455 as TDMC_INC_QUASI_CRCT_PCM, (chararray) $456 as TDMC_INC_QUASI_CRCT_TSL, (chararray) $457 as TDMC_INC_TERMID_INDEX, (chararray) $458 as TDMC_INC_TERMID_FORMAT, (chararray) $459 as TDMC_INC_TERMID_TYPE, (chararray) $460 as TDMC_INC_TERMID_UNION, (chararray) $461 as TDMC_INC_MGW_NAME_LENGTH, (chararray) $462 as TDMC_INC_MGW_NAME, (chararray) $463 as IC_BNC_CHAR, (chararray) $464 as CALL_START_INDICATION, (chararray) $465 as ENHANCED_OUTPULSED_NUMBER_LENGTH, (chararray) $466 as ENHANCED_OUTPULSED_NUMBER_TYPE, (chararray) $467 as ENHANCED_OUTPULSED_NUMBER_PLAN, (chararray) $468 as ENHANCED_OUTPULSED_NUMBER_PRESENTATION, (chararray) $469 as ENHANCED_OUTPULSED_NUMBER, (chararray) $470 as DESTINATION_ID, (chararray) $471 as SUBDESTINATION_ID_1, (chararray) $472 as ROUTE_NBR_1, (chararray) $473 as RESULT_1, (chararray) $474 as SUBDESTINATION_ID_2, (chararray) $475 as ROUTE_NBR_2, (chararray) $476 as RESULT_2, (chararray) $477 as SUBDESTINATION_ID_3, (chararray) $478 as ROUTE_NBR_3, (chararray) $479 as RESULT_3, (chararray) $480 as SUBDESTINATION_ID_4, (chararray) $481 as ROUTE_NBR_4, (chararray) $482 as RESULT_4, (chararray) $483 as SUBDESTINATION_ID_5, (chararray) $484 as ROUTE_NBR_5, (chararray) $485 as RESULT_5, (chararray) $486 as A_CIR_SEIZED_TIME, (chararray) $487 as A_CIR_RELEASED_TIME, (chararray) $488 as B_CIR_SEIZED_TIME, (chararray) $489 as B_CIR_RELEASED_TIME, (chararray) $490 as CM_SERV_REQ_TIME, (chararray) $491 as SETUP_TIME, (chararray) $492 as B_FIRST_INTERNAL_TERMINATION_ID_INDEX, (chararray) $493 as B_FIRST_INTERNAL_TERMINATION_ID_FORMAT, (chararray) $494 as B_FIRST_INTERNAL_TERMINATION_ID_TYPE, (chararray) $495 as B_FIRST_INTERNAL_TERMINATION_ID_UNION, (chararray) $496 as B_FIRST_UPD_INDEX, (chararray) $497 as GLOBAL_CALL_REF, (chararray) $498 as CALL_ID, (chararray) $499 as A_IN_SERVICE_KEY_1, (chararray) $500 as A_IN_SERVICE_STATUS_1, (chararray) $501 as A_IN_SERVICE_KEY_2, (chararray) $502 as A_IN_SERVICE_STATUS_2, (chararray) $503 as A_IN_SERVICE_KEY_3, (chararray) $504 as A_IN_SERVICE_STATUS_3, (chararray) $505 as A_IN_SERVICE_KEY_4, (chararray) $506 as A_IN_SERVICE_STATUS_4, (chararray) $507 as A_IN_SERVICE_KEY_5, (chararray) $508 as A_IN_SERVICE_STATUS_5, (chararray) $509 as A_IN_SERVICE_KEY_6, (chararray) $510 as A_IN_SERVICE_STATUS_6, (chararray) $511 as A_IN_SERVICE_KEY_7, (chararray) $512 as A_IN_SERVICE_STATUS_7, (chararray) $513 as A_IN_SERVICE_KEY_8, (chararray) $514 as A_IN_SERVICE_STATUS_8, (chararray) $515 as A_IN_SERVICE_KEY_9, (chararray) $516 as A_IN_SERVICE_STATUS_9, (chararray) $517 as A_IN_SERVICE_KEY_10, (chararray) $518 as A_IN_SERVICE_STATUS_10, (chararray) $519 as B_IN_SERVICE_KEY_1, (chararray) $520 as B_IN_SERVICE_STATUS_1, (chararray) $521 as B_IN_SERVICE_KEY_2, (chararray) $522 as B_IN_SERVICE_STATUS_2, (chararray) $523 as B_IN_SERVICE_KEY_3, (chararray) $524 as B_IN_SERVICE_STATUS_3, (chararray) $525 as B_IN_SERVICE_KEY_4, (chararray) $526 as B_IN_SERVICE_STATUS_4, (chararray) $527 as B_IN_SERVICE_KEY_5, (chararray) $528 as B_IN_SERVICE_STATUS_5, (chararray) $529 as B_IN_SERVICE_KEY_6, (chararray) $530 as B_IN_SERVICE_STATUS_6, (chararray) $531 as B_IN_SERVICE_KEY_7, (chararray) $532 as B_IN_SERVICE_STATUS_7, (chararray) $533 as B_IN_SERVICE_KEY_8, (chararray) $534 as B_IN_SERVICE_STATUS_8, (chararray) $535 as B_IN_SERVICE_KEY_9, (chararray) $536 as B_IN_SERVICE_STATUS_9, (chararray) $537 as B_IN_SERVICE_KEY_10, (chararray) $538 as B_IN_SERVICE_STATUS_10, (chararray) $539 as A_UMA_IP_ADDR_VER, (chararray) $540 as A_UMA_IP_ADDR, (chararray) $541 as A_UMA_IP_PORT, (chararray) $542 as B_UMA_IP_ADDR_VER, (chararray) $543 as B_UMA_IP_ADDR, (chararray) $544 as B_UMA_IP_PORT, (chararray) $545 as PAGING_LIFETIME, (chararray) $546 as LAST_SCP_GT_ADDRESS_LENGTH, (chararray) $547 as LAST_SCP_GT_ADDRESS_NUMBER_PLAN, (chararray) $548 as LAST_SCP_AP_VERSION, (chararray) $549 as LAST_SCP_DIALOG_SERVICE_KEY, (chararray) $550 as LAST_SCP_ROAMING_STATUS, (chararray) $551 as B_LAST_SCP_GT_ADDRESS_LENGTH, (chararray) $552 as B_LAST_SCP_GT_ADDRESS_NUMBER_PLAN, (chararray) $553 as B_LAST_SCP_AP_VERSION, (chararray) $554 as B_LAST_SCP_DIALOG_SERVICE_KEY, (chararray) $555 as B_LAST_SCP_ROAMING_STATUS, (chararray) $556 as PCM_1, (chararray) $557 as TSL_1, (chararray) $558 as IEIFIL_INDEX_1, (chararray) $559 as OWN_SYSTEM_1, (chararray) $560 as PCM_2, (chararray) $561 as TSL_2, (chararray) $562 as IEIFIL_INDEX_2, (chararray) $563 as OWN_SYSTEM_2, (chararray) $564 as PCM_3, (chararray) $565 as TSL_3, (chararray) $566 as IEIFIL_INDEX_3, (chararray) $567 as OWN_SYSTEM_3, (chararray) $568 as PCM_4, (chararray) $569 as TSL_4, (chararray) $570 as IEIFIL_INDEX_4, (chararray) $571 as OWN_SYSTEM_4, (chararray) $572 as PCM_5, (chararray) $573 as TSL_5, (chararray) $574 as IEIFIL_INDEX_5, (chararray) $575 as OWN_SYSTEM_5, (chararray) $576 as SUBDESTINATION_ID_6, (chararray) $577 as ROUTE_NBR_6, (chararray) $578 as PCM_6, (chararray) $579 as TSL_6, (chararray) $580 as IEIFIL_INDEX_6, (chararray) $581 as RESULT_6, (chararray) $582 as OWN_SYSTEM_6, (chararray) $583 as SUBDESTINATION_ID_7, (chararray) $584 as ROUTE_NBR_7, (chararray) $585 as PCM_7, (chararray) $586 as TSL_7, (chararray) $587 as IEIFIL_INDEX_7, (chararray) $588 as RESULT_7, (chararray) $589 as OWN_SYSTEM_7, (chararray) $590 as SUBDESTINATION_ID_8, (chararray) $591 as ROUTE_NBR_8, (chararray) $592 as PCM_8, (chararray) $593 as TSL_8, (chararray) $594 as IEIFIL_INDEX_8, (chararray) $595 as RESULT_8, (chararray) $596 as OWN_SYSTEM_8, (chararray) $597 as SUBDESTINATION_ID_9, (chararray) $598 as ROUTE_NBR_9, (chararray) $599 as PCM_9, (chararray) $600 as TSL_9, (chararray) $601 as IEIFIL_INDEX_9, (chararray) $602 as RESULT_9, (chararray) $603 as OWN_SYSTEM_9, (chararray) $604 as SUBDESTINATION_ID_10, (chararray) $605 as ROUTE_NBR_10, (chararray) $606 as PCM_10, (chararray) $607 as TSL_10, (chararray) $608 as IEIFIL_INDEX_10, (chararray) $609 as RESULT_10, (chararray) $610 as OWN_SYSTEM_10, (chararray) $611 as SUBDESTINATION_ID_11, (chararray) $612 as ROUTE_NBR_11, (chararray) $613 as PCM_11, (chararray) $614 as TSL_11, (chararray) $615 as IEIFIL_INDEX_11, (chararray) $616 as RESULT_11, (chararray) $617 as OWN_SYSTEM_11, (chararray) $618 as SUBDESTINATION_ID_12, (chararray) $619 as ROUTE_NBR_12, (chararray) $620 as PCM_12, (chararray) $621 as TSL_12, (chararray) $622 as IEIFIL_INDEX_12, (chararray) $623 as RESULT_12, (chararray) $624 as OWN_SYSTEM_12, (chararray) $625 as SUBDESTINATION_ID_13, (chararray) $626 as ROUTE_NBR_13, (chararray) $627 as PCM_13, (chararray) $628 as TSL_13, (chararray) $629 as IEIFIL_INDEX_13, (chararray) $630 as RESULT_13, (chararray) $631 as OWN_SYSTEM_13, (chararray) $632 as SUBDESTINATION_ID_14, (chararray) $633 as ROUTE_NBR_14, (chararray) $634 as PCM_14, (chararray) $635 as TSL_14, (chararray) $636 as IEIFIL_INDEX_14, (chararray) $637 as RESULT_14, (chararray) $638 as OWN_SYSTEM_14, (chararray) $639 as SUBDESTINATION_ID_15, (chararray) $640 as ROUTE_NBR_15, (chararray) $641 as PCM_15, (chararray) $642 as TSL_15, (chararray) $643 as IEIFIL_INDEX_15, (chararray) $644 as RESULT_15, (chararray) $645 as OWN_SYSTEM_15, (chararray) $646 as SUBDESTINATION_ID_16, (chararray) $647 as ROUTE_NBR_16, (chararray) $648 as PCM_16, (chararray) $649 as TSL_16, (chararray) $650 as IEIFIL_INDEX_16, (chararray) $651 as RESULT_16, (chararray) $652 as OWN_SYSTEM_16, (chararray) $653 as SUBDESTINATION_ID_17, (chararray) $654 as ROUTE_NBR_17, (chararray) $655 as PCM_17, (chararray) $656 as TSL_17, (chararray) $657 as IEIFIL_INDEX_17, (chararray) $658 as RESULT_17, (chararray) $659 as OWN_SYSTEM_17, (chararray) $660 as SUBDESTINATION_ID_18, (chararray) $661 as ROUTE_NBR_18, (chararray) $662 as PCM_18, (chararray) $663 as TSL_18, (chararray) $664 as IEIFIL_INDEX_18, (chararray) $665 as RESULT_18, (chararray) $666 as OWN_SYSTEM_18, (chararray) $667 as SUBDESTINATION_ID_19, (chararray) $668 as ROUTE_NBR_19, (chararray) $669 as PCM_19, (chararray) $670 as TSL_19, (chararray) $671 as IEIFIL_INDEX_19, (chararray) $672 as RESULT_19, (chararray) $673 as OWN_SYSTEM_19, (chararray) $674 as SUBDESTINATION_ID_20, (chararray) $675 as ROUTE_NBR_20, (chararray) $676 as PCM_20, (chararray) $677 as TSL_20, (chararray) $678 as IEIFIL_INDEX_20, (chararray) $679 as RESULT_20, (chararray) $680 as OWN_SYSTEM_20, (chararray) $681 as A_ACCESS_SIDE_IP_ADDR_VER, (chararray) $682 as A_ACCESS_SIDE_IP_ADDR, (chararray) $683 as A_ACCESS_SIDE_IP_PORT, (chararray) $684 as B_ACCESS_SIDE_IP_ADDR_VER, (chararray) $685 as B_ACCESS_SIDE_IP_ADDR, (chararray) $686 as B_ACCESS_SIDE_IP_PORT, (chararray) $687 as ICID_LENGTH, (chararray) $688 as ICID_OVERFLOW, (chararray) $689 as ICID, (chararray) $690 as A_USED_CODEC_LIST_CODEC_AMOUNT, (chararray) $691 as A_USED_CODEC_1, (chararray) $692 as A_USED_CODEC_TIME_1, (chararray) $693 as A_USED_CODEC_2, (chararray) $694 as A_USED_CODEC_TIME_2, (chararray) $695 as A_USED_CODEC_3, (chararray) $696 as A_USED_CODEC_TIME_3, (chararray) $697 as A_USED_CODEC_4, (chararray) $698 as A_USED_CODEC_TIME_4, (chararray) $699 as A_USED_CODEC_5, (chararray) $700 as A_USED_CODEC_TIME_5, (chararray) $701 as A_USED_CODEC_6, (chararray) $702 as A_USED_CODEC_TIME_6, (chararray) $703 as A_USED_CODEC_7, (chararray) $704 as A_USED_CODEC_TIME_7, (chararray) $705 as A_USED_CODEC_8, (chararray) $706 as A_USED_CODEC_TIME_8, (chararray) $707 as A_USED_CODEC_9, (chararray) $708 as A_USED_CODEC_TIME_9, (chararray) $709 as A_USED_CODEC_10, (chararray) $710 as A_USED_CODEC_TIME_10, (chararray) $711 as B_USED_CODEC_LIST_CODEC_AMOUNT, (chararray) $712 as B_USED_CODEC_1, (chararray) $713 as B_USED_CODEC_TIME_1, (chararray) $714 as B_USED_CODEC_2, (chararray) $715 as B_USED_CODEC_TIME_2, (chararray) $716 as B_USED_CODEC_3, (chararray) $717 as B_USED_CODEC_TIME_3, (chararray) $718 as B_USED_CODEC_4, (chararray) $719 as B_USED_CODEC_TIME_4, (chararray) $720 as B_USED_CODEC_5, (chararray) $721 as B_USED_CODEC_TIME_5, (chararray) $722 as B_USED_CODEC_6, (chararray) $723 as B_USED_CODEC_TIME_6, (chararray) $724 as B_USED_CODEC_7, (chararray) $725 as B_USED_CODEC_TIME_7, (chararray) $726 as B_USED_CODEC_8, (chararray) $727 as B_USED_CODEC_TIME_8, (chararray) $728 as B_USED_CODEC_9, (chararray) $729 as B_USED_CODEC_TIME_9, (chararray) $730 as B_USED_CODEC_10, (chararray) $731 as B_USED_CODEC_TIME_10, (chararray) $732 as A_MSC_INTERNAL_CELL, (chararray) $733 as A_FIRST_MSC_INTERNAL_CELL, (chararray) $734 as B_MSC_INTERNAL_CELL, (chararray) $735 as B_FIRST_MSC_INTERNAL_CELL, (chararray) $736 as A_ELEMENT_TYPE, (chararray) $737 as B_ELEMENT_TYPE, (chararray) $738 as USED_MMTEL_SERVICES, (chararray) $739 as SRVCC_CALL, (chararray) $740 as WIDEBAND_TRANSCODING_USED, (chararray) $741 as TRAF_PRIKEY_AUXCNTRCOL;

data = FILTER data BY EVENT_TYPE == 16777216 and CELL !=0;

jnd  = JOIN data BY MSISDN_NUMBER  , rtt_data BY B_DIRECTION_NUMBER ;

grpd = GROUP jnd BY (B_DIRECTION_NUMBER,B_IMEI, REPORT_TIME, RTT_REPORT_TIME, B_CELL, B_DX_CAUSE);
data = FOREACH grpd GENERATE FLATTEN(group) AS (B_DIRECTION_NUMBER, B_IMEI , REPORT_TIME , RTT_REPORT_TIME , B_CELL , B_DX_CAUSE) ;

grpd = GROUP data BY (B_DIRECTION_NUMBER, B_IMEI);
grpd = FOREACH grpd {
      grop = ORDER data BY REPORT_TIME  desc ;
      GENERATE group, grop ;
}

data = FOREACH grpd GENERATE FLATTEN(group) as (B_DIRECTION_NUMBER, B_IMEI) ,FLATTEN(ReportTimeGroup(grop)) AS (REPORT_TIME:chararray , RTT_REPORT_TIME:chararray, B_CELL:chararray , B_DX_CAUSE:chararray , endTime:chararray, flag:int) ;

data = FILTER data By flag == 1;

grpd = GROUP data BY (B_DIRECTION_NUMBER, B_IMEI , REPORT_TIME);
grpd = FOREACH grpd {
        grop = ORDER data BY REPORT_TIME ;
        GENERATE group, grop, COUNT(data.REPORT_TIME) as cnt  ;
}
data = FOREACH grpd GENERATE  flatten(grop) , cnt  ;

grpd = GROUP data BY(B_DIRECTION_NUMBER ,B_IMEI, REPORT_TIME, cnt,  B_DX_CAUSE, B_CELL);
data = FOREACH grpd GENERATE FLATTEN(group) as (B_DIRECTION_NUMBER , B_IMEI, REPORT_TIME, cnt,  B_DX_CAUSE, B_CELL) , COUNT(data.B_DIRECTION_NUMBER) as cnt_detail ;

STORE data INTO '$output' USING PigStorage(',');

