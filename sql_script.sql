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
