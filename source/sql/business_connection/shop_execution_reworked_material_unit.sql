SELECT
       bill_entry.FPKID AS "多选基础资料id",
       bill_entry.FEntryId AS "对应单据分录id",
       bill.fid AS "对应单据id",
       bill_entry.FBasedataId AS "对应基础资料id"
FROM
       crrc_secd.tk_crrc_back_reqcg bill_entry
       LEFT JOIN crrc_secd.tk_crrc_backmaterial bill ON bill.FEntryId = bill_entry.FEntryId