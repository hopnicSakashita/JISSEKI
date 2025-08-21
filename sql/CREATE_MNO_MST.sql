/* モノマーマスタ */
CREATE TABLE `MNO_MST` (
    `MNO_SYU` varchar(1) NOT NULL COMMENT 'モノマー種',
    `MNO_NM` varchar(30) COMMENT 'モノマー名',
    `MNO_TARGET` decimal(4,1) COMMENT '目標値',
    PRIMARY KEY (`MNO_SYU`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='モノマーマスタ';