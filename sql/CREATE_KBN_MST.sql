/* 区分マスタ */
CREATE TABLE `KBN_MST` (
    `KBN_TYP` varchar(4) NOT NULL COMMENT '区分種別',
    `KBN_ID` decimal(3) NOT NULL COMMENT '区分ID',
    `KBN_NM` varchar(30) COMMENT '区分名',
    PRIMARY KEY (`KBN_TYP`, `KBN_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='区分マスタ';