SELECT
       bill.FPKID AS "多选基础资料id",
       bill.FId AS "对应单据id",
       bill.FBasedataId AS "对应基础资料id"
FROM
       crrc_secd.tk_crrc_chgbill_in_dept bill