SELECT
       bill.fid AS "id",
       bill.FEntryId AS "单据分录id",
       bill.FSeq AS "单据分录序号",
       bill.fk_crrc_begintrack_e2 AS "开始车号",
       bill.fk_crrc_endtrack_e2 AS "结束车号",
       bill.fk_crrc_chgbill_jch_e2 AS "节车号",
       bill.fk_crrc_changecar_e2 AS "执行车次",
       bill.fk_crrc_serialno AS "序列号",
       bill.fk_crrc_craftnext_e2 AS "工艺步骤",
       bill.fk_crrc_finishdate_e2 AS "要求完成日期",
       bill.fk_crrc_finishnode_e2 AS "完成时间节点",
       bill.fk_crrc_worktime AS "返工工时（分钟）",
       case bill.fk_crrc_issubmaterial
              WHEN 0 THEN '是'
              WHEN 1 THEN '否'
       end AS "是否退料"
FROM
       crrc_secd.tk_crrc_chggyentry bill