/* 製品マスタ */
CREATE TABLE `PRD_MST` (
    `PRD_ID` varchar(5) NOT NULL COMMENT '製品ID',
    `PRD_KBN` decimal(2) COMMENT '商品分類',
    `PRD_TYP` varchar(1) COMMENT '識別ID',
    `PRD_NM` varchar(60) COMMENT '製品名',
    `PRD_COLOR` varchar(20) COMMENT '膜カラー',
    `PRD_PLY_DAYS` decimal(2) COMMENT '重合日数',

    PRIMARY KEY (`PRD_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='製品マスタ';

alter table PRD_MST modify column PRD_ID varchar(5) NOT NULL COMMENT '製品ID';