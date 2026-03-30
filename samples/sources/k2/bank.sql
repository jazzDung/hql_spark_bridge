-- Kết nối vào container bằng công cụ như Azure Data Studio hoặc DBeaver
CREATE DATABASE k2;
GO
USE k2;
GO
CREATE TABLE dbo.bank (
    bankid VARCHAR(50),
    localbankcode VARCHAR(50),
    bankname NVARCHAR(255),
    swiftcode VARCHAR(50),
    oribankid VARCHAR(50),
    approveuser BIGINT,
    approvets DATETIME,
    createuser BIGINT,
    createts DATETIME,
    updateuser BIGINT,
    updatets DATETIME,
    recstatus VARCHAR(10)
);
-- Chèn dữ liệu mẫu để test trim/nvl
INSERT INTO dbo.bank (bankid, localbankcode, bankname)
VALUES ('B001', ' 12345 ', 'Kenanga Bank');