CREATE TABLE [Zlecenia] (
  [ZlecenieId] bigint PRIMARY KEY,
  [ZlecenieRodzaj] varchar(10),
  [DataUtworzenia] date,
  [CzasUtworzenia] time
)
GO

CREATE TABLE [Zawiadomienia] (
  [ZawiadomienieId] int PRIMARY KEY,
  [ZawiadomienieRodzaj] varchar(10),
  [ZlecenieId] bigint,
  [Lokalizacja] varchar(20),
  [LokalizacjaFunkcjonalnaId] varchar(30),
  [UrzadzenieId] int,
  [DataUtworzenia] date,
  [UszkodzenieId] int,
  [PrzyczynaId] int,
  [DataPoczatkuZaklocenia] date,
  [DataKoncaZaklocenia] date,
  [CzasPoczatkuZaklocenia] time,
  [CzasKoncaZaklocenia] time,
  [Przestoj] char(1),
  [CzasPrzestoju] float,
  [JednostkaCzasu] varchar(10)
)
GO

CREATE TABLE [Linie] (
  [LiniaId] int PRIMARY KEY,
  [LiniaNazwa] varchar(50)
)
GO

CREATE TABLE [LokalizacjaFunkcjonalna] (
  [LokalizacjaFunkcjonalnaId] varchar(30) PRIMARY KEY,
  [LokalizacjaFunkcjonalnaNazwa] varchar(50),
  [LiniaId] int
)
GO

CREATE TABLE [Przyczyny] (
  [PrzyczynaId] int PRIMARY KEY,
  [PrzyczynaNazwa] varchar(50)
)
GO

CREATE TABLE [Uszkodzenia] (
  [UszkodzenieId] int PRIMARY KEY,
  [UszkodzenieNazwa] varchar(50)
)
GO

CREATE TABLE [Urzadzenia] (
  [UrzadzenieId] int PRIMARY KEY,
  [UrzadzenieNazwa] varchar(50)
)
GO

CREATE TABLE [RodzajeZawiadomienia] (
  [ZawiadomienieRodzaj] varchar(10) PRIMARY KEY,
  [ZawiadomienieRodzajNazwa] varchar(50)
)
GO

CREATE TABLE [RodzajeZlecenia] (
  [ZlecenieRodzaj] varchar(10) PRIMARY KEY,
  [ZlecenieRodzajNazwa] varchar(50)
)
GO

CREATE TABLE [Data] (
  [Data] date PRIMARY KEY,
  [Rok] int,
  [Miesiac] int,
  [MiesiacNazwa] varchar(20),
  [Tydzien] int,
  [DzienTygodnia] int,
  [DzienTygodniaNazwa] varchar(20),
  [Kampania] varchar(9)
)
GO

CREATE TABLE [BilansProdukcji] (
  [Od] date,
  [Do] date,
  [Linia] int,
  [Rodzina] varchar(20),
  [QLTOTAkt] float,
  [QLTOTPln] float,
  [ProcentDvtProduk] float,
  [ZmianaCzysty] float,
  [ZmianaPrg] float,
  [ZmianaStd] float,
  [QZmAkt] float,
  [QZmDocel] float,
  [QZmStd] float,
  [QCPKAkt] float,
  [QCPKDocel] float,
  [QCPKStd] float,
  [OpeLNShAkt] float,
  [OpeLNShDocel] float,
  [OpeLNShStd] float,
  [OpeELShAkt] float,
  [OpeELShDocel] float,
  [OpeELShStd] float,
  [GQLAkt] float,
  [GQLDocel] float,
  [GQLStd] float,
  [ProcentSCEff] float,
  [ProcentSCStd] float,
  [ProcentSREff] float,
  [ProcentSRStd] float,
  [ProcentSFSPEff] float,
  [ProcentSFSPStd] float,
  [GodzPracAkt] float,
  [GodzPracDocel] float,
  [GodzPracStd] float,
  [ProcentELINIAEff] float,
  [ProcentELINIAObb] float,
  [ProcentELINIAStd] float,
  [ProcentEPracEff] float,
  [ProcentEPracObb] float,
  [ProcentEPracStd] float,
  [ProcentZyskuEff] float,
  [ProcentZyskuObb] float,
  [ProcentZyskuStd] float
)
GO

ALTER TABLE [Zawiadomienia] ADD FOREIGN KEY ([ZlecenieId]) REFERENCES [Zlecenia] ([ZlecenieId])
GO

ALTER TABLE [Zawiadomienia] ADD FOREIGN KEY ([PrzyczynaId]) REFERENCES [Przyczyny] ([PrzyczynaId])
GO

ALTER TABLE [Zawiadomienia] ADD FOREIGN KEY ([UszkodzenieId]) REFERENCES [Uszkodzenia] ([UszkodzenieId])
GO

ALTER TABLE [Zawiadomienia] ADD FOREIGN KEY ([UrzadzenieId]) REFERENCES [Urzadzenia] ([UrzadzenieId])
GO

ALTER TABLE [Zawiadomienia] ADD FOREIGN KEY ([ZawiadomienieRodzaj]) REFERENCES [RodzajeZawiadomienia] ([ZawiadomienieRodzaj])
GO

ALTER TABLE [Zlecenia] ADD FOREIGN KEY ([ZlecenieRodzaj]) REFERENCES [RodzajeZlecenia] ([ZlecenieRodzaj])
GO

ALTER TABLE [Zawiadomienia] ADD FOREIGN KEY ([DataUtworzenia]) REFERENCES [Data] ([Data])
GO

ALTER TABLE [Zawiadomienia] ADD FOREIGN KEY ([LokalizacjaFunkcjonalnaId]) REFERENCES [LokalizacjaFunkcjonalna] ([LokalizacjaFunkcjonalnaId])
GO

ALTER TABLE [LokalizacjaFunkcjonalna] ADD FOREIGN KEY ([LiniaId]) REFERENCES [Linie] ([LiniaId])
GO

ALTER TABLE [BilansProdukcji] ADD FOREIGN KEY ([Linia]) REFERENCES [Linie] ([LiniaId])
GO

ALTER TABLE [BilansProdukcji] ADD FOREIGN KEY ([Od]) REFERENCES [Data] ([Data])
GO

ALTER TABLE [BilansProdukcji] ADD FOREIGN KEY ([Do]) REFERENCES [Data] ([Data])
GO

ALTER TABLE [Zawiadomienia] ADD FOREIGN KEY ([DataPoczatkuZaklocenia]) REFERENCES [Data] ([Data])
GO

ALTER TABLE [Zawiadomienia] ADD FOREIGN KEY ([DataKoncaZaklocenia]) REFERENCES [Data] ([Data])
GO

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