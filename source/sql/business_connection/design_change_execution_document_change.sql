SELECT
       bill.FId AS "id",
       bill.FEntryId AS "单据分录id",
       bill.FSeq AS "单据分录序号",
       case bill.fk_crrc_type_e4
              WHEN 'A' THEN '图样'
              WHEN 'B' THEN '线表'
              WHEN 'C' THEN '其他'
              WHEN 'D' THEN '物料清单'
       end AS "类型",
       bill.fk_crrc_number_e4 AS "编号",
       bill.fk_crrc_filename AS "名称",
       bill.fk_crrc_oldversion AS "旧版",
       bill.fk_crrc_newversion AS "新版",
       bill.fk_crrc_integerfield AS "张数",
       bill.fk_crrc_filename_e4 AS "附件名称"
FROM
       crrc_secd.tk_crrc_filechgentry bill