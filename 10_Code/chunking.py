"""Chunking the prescription data for the necessary states."""
import pandas as pd

# Chunk data
def chunking(states):
    # Chunk data by state
    df = pd.DataFrame()
    with pd.read_csv(
        "C:\\Users\\nicho\\Downloads\\prescription_data.zip",
        # delimiter="\t",
        chunksize=2_500_000,
        iterator=True,
        low_memory=False,
        usecols=[
            "BUYER_STATE",
            "BUYER_COUNTY",
            "QUANTITY",
            "NDC_NO",
            "DRUG_NAME",
            "TRANSACTION_DATE",
            "UNIT",
            "STRENGTH",
            "DOSAGE_UNIT",
            "CALC_BASE_WT_IN_GM",
            "MME_Conversion_Factor",
            "Measure",
            "dos_str",
        ],
        parse_dates=["TRANSACTION_DATE"],
    ) as reader:
        for chunk in reader:
            df = pd.concat(
                [df, chunk.loc[chunk.loc[:, "BUYER_STATE"].isin(states), :]],
                ignore_index=True,
            )

    # Convert numeric columns to floats
    floatColumns = ["QUANTITY", "CALC_BASE_WT_IN_GM", "MME_Conversion_Factor", "STRENGTH", "DOSAGE_UNIT", "dos_str"]
    for eachColumn in floatColumns:
        df[eachColumn] = df[eachColumn].astype("float64")

    # Convert date to datetime
    df['TRANSACTION_DATE'] = pd.to_datetime(df["TRANSACTION_DATE"], format = '%m%d%Y')

    return df


# Export data to parquet
def exportData(df, states):
    for eachState in states:
        # Exclude Unit column because it is creating errors
        df.loc[df.loc[:, "BUYER_STATE"] == eachState, df.columns[df.columns != "UNIT"]].to_parquet(
            "C:\\Users\\nicho\\Documents\\GitHub\\pds-2022-grey-team\\20_Intermediate_Files\\shipment"
            + eachState
            + ".parquet" , 
            engine = 'fastparquet'
        )


if __name__ == "__main__":
    # Create a list of states to chunk, ram for my computer can typically handle 3 states at a time
    stateList = ["PA", "OH", "GA"]
    data = chunking(stateList)
    exportData(data, states)