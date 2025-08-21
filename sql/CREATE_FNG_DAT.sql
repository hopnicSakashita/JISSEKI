/* 膜不良データ */
CREATE TABLE `FNG_DAT` (
    `FNG_LOT_NO` varchar(16) NOT NULL COMMENT 'ロットNo',
    `FNG_NG_ID` decimal(1) NOT NULL COMMENT '不良項目',
    `FNG_INS_QTY` decimal(4) COMMENT '検査数',
    `FNG_NG_QTY` decimal(4) COMMENT '不良数',
    `FNG_BIKO` varchar(50) COMMENT '備考',
    PRIMARY KEY (`FNG_LOT_NO`, `FNG_NG_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='膜不良データ';