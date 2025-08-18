CREATE TABLE [Zmiany] (
  [ZmianaId] int,
  [Data] date,
  [KierownikId] int,
  [IloscGodzin] int,
  [NumerZmiany] int,
  [StatusZmianyId] int,
  [UGPId] varchar(20)
)
GO

CREATE TABLE [Przestoje] (
  [PrzestojId] int,
  [CzasPrzestoju] time,
  [UrzadzenieId] int,
  [ZmianaId] int
)
GO

CREATE TABLE [Urzadzenia] (
  [UrzadzenieId] int,
  [UrzadzenieNazwa] varchar(50),
  [LokalizacjaFunkcjonalnaId] varchar(30)
)
GO

CREATE TABLE [UGP] (
  [UGPId] varchar(20),
  [UGPNazwa] varchar(50)
)
GO

CREATE TABLE [Kierownicy] (
  [KierownikId] int,
  [KierownikNazwa] varchar(50)
)
GO

CREATE TABLE [StatusyZmiany] (
  [StatusZmianyId] int,
  [StatusZmianyNazwa] varchar(50)
)
GO

CREATE TABLE [Data] (
  [Data] date,
  [Rok] int,
  [Miesiac] int,
  [MiesiacNazwa] varchar(20),
  [Tydzien] int,
  [DzienTygodnia] int,
  [DzienTygodniaNazwa] varchar(20),
  [Kampania] varchar(9)
)
GO

CREATE TABLE [LokalizacjaFunkcjonalna] (
  [LokalizacjaFunkcjonalnaId] varchar(30),
  [LokalizacjaFunkcjonalnaNazwa] varchar(50),
  [LiniaId] int
)
GO

CREATE TABLE [Linie] (
  [LiniaId] int,
  [LiniaNazwa] varchar(50),
  [UGPId] varchar(20)
)
GO

ALTER TABLE [Zmiany] ADD FOREIGN KEY ([UGPId]) REFERENCES [UGP] ([UGPId])
GO

ALTER TABLE [Przestoje] ADD FOREIGN KEY ([ZmianaId]) REFERENCES [Zmiany] ([ZmianaId])
GO

ALTER TABLE [Zmiany] ADD FOREIGN KEY ([StatusZmianyId]) REFERENCES [StatusyZmiany] ([StatusZmianyId])
GO

ALTER TABLE [Zmiany] ADD FOREIGN KEY ([KierownikId]) REFERENCES [Kierownicy] ([KierownikId])
GO

ALTER TABLE [Zmiany] ADD FOREIGN KEY ([Data]) REFERENCES [Data] ([Data])
GO

ALTER TABLE [Przestoje] ADD FOREIGN KEY ([UrzadzenieId]) REFERENCES [Urzadzenia] ([UrzadzenieId])
GO

ALTER TABLE [Urzadzenia] ADD FOREIGN KEY ([LokalizacjaFunkcjonalnaId]) REFERENCES [LokalizacjaFunkcjonalna] ([LokalizacjaFunkcjonalnaId])
GO

ALTER TABLE [LokalizacjaFunkcjonalna] ADD FOREIGN KEY ([LiniaId]) REFERENCES [Linie] ([LiniaId])
GO

ALTER TABLE [Linie] ADD FOREIGN KEY ([UGPId]) REFERENCES [UGP] ([UGPId])
GO

SET LANGUAGE Polish;
DECLARE @StartDate DATE = '2010-01-01';
DECLARE @EndDate DATE = '2030-12-31';
SET DATEFIRST 1;

WITH Dates AS (
    SELECT @StartDate AS Data
    UNION ALL
    SELECT DATEADD(DAY, 1, Data)
    FROM Dates
    WHERE Data < @EndDate
)

INSERT INTO Data (
    Data, Rok, Miesiac, MiesiacNazwa,
    Tydzien, DzienTygodnia, DzienTygodniaNazwa, Kampania
)

SELECT 
    Data,
    YEAR(Data) AS Rok,
    MONTH(Data) AS Miesiac,
    DATENAME(MONTH, Data) AS MiesiacNazwa,
    DATEPART(WEEK, Data) AS Tydzien,
    DATEPART(WEEKDAY, Data) AS DzienTygodnia,
    DATENAME(WEEKDAY, Data) AS DzienTygodniaNazwa,

    -- KAMPANIA
    CAST(
        CASE 
            WHEN MONTH(Data) >= 9 THEN YEAR(Data)
            ELSE YEAR(Data) - 1
        END AS VARCHAR(4)
    ) + '/' + 
    CAST(
        CASE 
            WHEN MONTH(Data) >= 9 THEN YEAR(Data) + 1
            ELSE YEAR(Data)
        END AS VARCHAR(4)
    ) AS Kampania

FROM Dates
OPTION (MAXRECURSION 32767);