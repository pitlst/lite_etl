SELECT  bill.FId         AS "对应单据id"
       ,bill.FPKID       AS "多选基础资料id"
       ,bill.FBasedataId AS "对应基础资料id"
FROM crrc_secd.tk_crrc_book_copyunit bill