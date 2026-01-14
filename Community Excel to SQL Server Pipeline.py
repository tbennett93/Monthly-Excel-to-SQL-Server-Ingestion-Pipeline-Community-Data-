import pandas as pd
import datetime
from pathlib import Path
import pyodbc


def calc_target_data_date() -> datetime.datetime:
    today = datetime.datetime.now().date()
    this_month = today.month
    prev_month = this_month - 1 if this_month > 1 else 12
    year = today.year - 1 if this_month == 1 else today.year 
    return datetime.datetime(year, prev_month, 1).date()


def date_validation(df: pd.DataFrame, target_data_date: datetime.date):
    df["Date"] = pd.to_datetime(df["Date"], errors='coerce',format="%d/%m/%Y")

    if df["Date"].isna().any():
        raise ValueError("Invalid date value entered - please review. Dates must by DD/MM/YYYY")

    if not (
        df["Date"]
        .dt
        .date
        .eq(target_data_date)
        .any()
    ):
        raise ImportWarning(f"Data expected for {target_data_date} but none found - please investigate")
    
    return df


def validation_reject_non_allowed_values(df: pd.DataFrame, filepath: Path, *, sheet_name: str, column_name: str):
    df_allowed_values = pd.read_excel(filepath, sheet_name=sheet_name, header=None).iloc[:,0]
    invalid_values = (
        df[~df[column_name].isin(df_allowed_values)]
        .drop_duplicates()
        .copy()
    )

    if not invalid_values.empty:
        raise ValueError(f"Unmapped {column_name} values, please investigate: \n {invalid_values}")


def process_in_sql(df: pd.DataFrame, cursor: pyodbc.Cursor):
    cursor.execute("truncate table dbo.[Tbl_Dashboard_Data_STAGING]")
    cursor.execute("truncate table dbo.[Tbl_Dashboard_Data_STAGING_Community]")
    values = df.itertuples(index=False, name=None)
    cursor.executemany(
        """
        INSERT INTO dbo.[Tbl_Dashboard_Data_STAGING_Community] (
        [IndicatorName]
        ,[Period]
        ,[Date]
        ,[DataValue]
        ,[DataValue2]
        ,[Specialty]
        ,[source]
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        values
    )
    cursor.execute("exec dbo.sp_dashboard_data_community")
    cursor.execute('exec dbo.sp_dashboard_data_process_from_staging')
    cursor.commit()    


def load_source(filepath:Path) -> pd.DataFrame:
    #define dataframe
    df = pd.read_excel(filepath,sheet_name="Sheet1")
    df: pd.DataFrame = df[["IndicatorName","Period","Date","DataValue","DataValue2","Speciality","Source"]].copy()
    return df


def validate(df: pd.DataFrame, filepath:Path, target_data_date:datetime.date, EXPECTED_COLUMNS:dict) -> pd.DataFrame:

    #validation
    df = date_validation(df, target_data_date)
    validation_reject_non_allowed_values(df, filepath, sheet_name="Indicator Name", column_name="IndicatorName")
    validation_reject_non_allowed_values(df, filepath, sheet_name="Specialties", column_name="Speciality")
    validation_reject_non_allowed_values(df, filepath, sheet_name="Sources", column_name="Source")

    #set types
    for column, type in EXPECTED_COLUMNS.items():
        df[column] = df[column].astype(type)
    return df


def load_to_sql(df):
    #load
    with  pyodbc.connect(
        "DRIVER=XXX;"
        "SERVER=XXX;"
        "DATABASE=XXX;"
    ) as conn:
        
        cursor = conn.cursor()
        process_in_sql(df, cursor)


def main():

    EXPECTED_COLUMNS = {
    "IndicatorName": "string",
    "Period": "string",
    "Date": "datetime64[ns]",
    "DataValue": "float",
    "DataValue2": "float",
    "Speciality": "string",
    "Source": "string"
    }


    target_data_date = calc_target_data_date()

    #get file details
    FOLDER = Path(r"Sample Data") 
    filename = datetime.date.strftime(target_data_date, "%B %Y-Community.xlsx")
    filepath = FOLDER / filename

    df = load_source(filepath)
    df = validate(df, filepath, target_data_date, EXPECTED_COLUMNS)
    load_to_sql(df)

if __name__ == "__main__":
    main()
