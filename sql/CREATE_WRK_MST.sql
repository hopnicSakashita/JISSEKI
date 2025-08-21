/* 作業員マスタ */
CREATE TABLE `WRK_MST` (
    `WRK_ID` decimal(3) NOT NULL COMMENT 'ID',
    `WRK_NM` nvarchar(50) COMMENT '作業員名',
    PRIMARY KEY (`WRK_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='作業員マスタ';