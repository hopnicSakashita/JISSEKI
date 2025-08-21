/* 指示データ */
CREATE TABLE `SJI_DAT` (
    `SJI_PRD_ID` varchar(5) COMMENT '製品ID',
    `SJI_DATE` datetime COMMENT '指示日',
    `SJI_QTY` decimal(4) COMMENT '指示数',
    PRIMARY KEY (`SJI_PRD_ID`,`SJI_DATE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='指示データ';

alter table SJI_DAT modify column SJI_PRD_ID varchar(5) COMMENT '製品ID';