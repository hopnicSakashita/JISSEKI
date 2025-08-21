/* 機械マスタ */
CREATE TABLE `MCN_MST` (
    `MCN_ID` decimal(3) NOT NULL COMMENT 'ID',
    `MCN_NM` nvarchar(50) COMMENT '作業員名',
    PRIMARY KEY (`MCN_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='機械マスタ';