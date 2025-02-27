SELECT
       bill.fid AS "id",
       bill.fbillno AS "变更单号",
       bill.fk_crrc_chgbillname AS "变更单名称",
       bill.fk_crrc_project AS "项目号",
       bill.fk_crrc_projectname AS "项目名称",
       bill.fk_crrc_designbillnum AS "关联设计变更单编码",
       bill.fk_crrc_designbillname AS "关联设计变更单名称",
       bill.fk_crrc_towning_user AS "发起人",
       bill.fk_crrc_dowing_user AS "设计变更用户",
       bill.fk_crrc_date_released AS "发起时间",
       bill.fk_crrc_ddate_released AS "设计变更发起时间",
       case bill.fk_crrc_obstatus
              WHEN 'A' THEN '已分派'
              WHEN 'B' THEN '未分派'
              WHEN 'C' THEN '已完成'
       end AS "任务状态",
       _user.fnumber AS "分派用户工号",
       _user.FTRUENAME AS "分派用户名称",
       bill.fk_crrc_sendunit AS "发送单位",
       bill.fk_crrc_sendoutdept AS "发出单位",
       bill.fk_crrc_jobdesc AS "描述",
       case bill.fk_crrc_operation
              WHEN 'A' THEN '正常分派'
              WHEN 'B' THEN '驳回'
       end AS "操作",
       bill.fmodifytime AS "修改时间"
FROM
       crrc_secd.tk_crrc_craftchangebill bill
       LEFT JOIN crrc_sys.t_sec_user _user ON bill.fk_crrc_senduser = _user.FID