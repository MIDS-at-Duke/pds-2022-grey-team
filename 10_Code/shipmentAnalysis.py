"""Analyze shipments of opioids to pharmacies in the US."""

import pandas as pd
import altair as alt
import numpy as np

# Import shipment data and group to a manageable size
def mergeStates(states):
    groupedShipments = pd.DataFrame()
    for state in states:
        shipments = pd.read_parquet(
            "C:\\Users\\nicho\\Documents\\GitHub\\pds-2022-grey-team\\20_Intermediate_Files\\shipment"
            + state
            + ".parquet"
        )
        shipments["Converted Units"] = (
            shipments["CALC_BASE_WT_IN_GM"] * 1000 * shipments["MME_Conversion_Factor"]
        )
        shipments.loc[:, "Year"] = shipments.loc[:, "TRANSACTION_DATE"].dt.year
        groupedShipments = pd.concat(
            [
                groupedShipments,
                shipments.groupby(
                    ["BUYER_STATE", "BUYER_COUNTY", "Year"], as_index=False
                ).sum(numeric_only=True),
            ],
            ignore_index=True,
        )
        shipments = 0  # Clear memory
    return groupedShipments


# Import population data
def censusData():
    # Import census data 2000-2010
    census00s = pd.read_csv(
        "C:\\Users\\nicho\\Documents\\GitHub\\pds-2022-grey-team\\00_Source\\Population\\population2000-2010.csv",
        usecols=[
            "STNAME",
            "CTYNAME",
            "POPESTIMATE2000",
            "POPESTIMATE2001",
            "POPESTIMATE2002",
            "POPESTIMATE2003",
            "POPESTIMATE2004",
            "POPESTIMATE2005",
            "POPESTIMATE2006",
            "POPESTIMATE2007",
            "POPESTIMATE2008",
            "POPESTIMATE2009",
        ],
        encoding_errors="replace",
    )

    # Import census data 2010-2020
    census10s = pd.read_csv(
        "C:\\Users\\nicho\\Documents\\GitHub\\pds-2022-grey-team\\00_Source\\Population\\population2010-2020.csv",
        usecols=[
            "STNAME",
            "CTYNAME",
            "CENSUS2010POP",
            "POPESTIMATE2011",
            "POPESTIMATE2012",
            "POPESTIMATE2013",
            "POPESTIMATE2014",
            "POPESTIMATE2015",
            "POPESTIMATE2016",
            "POPESTIMATE2017",
            "POPESTIMATE2018",
            "POPESTIMATE2019",
            "POPESTIMATE2020",
        ],
        encoding_errors="replace",
    )

    # Merge census data
    census = census00s.merge(census10s, on=["STNAME", "CTYNAME"])

    # Load state abbrevaitions because this is how state is stored in shipment data
    abbreviations = pd.read_html(
        "https://simple.wikipedia.org/wiki/List_of_U.S._states_by_traditional_abbreviation"
    )[0]

    # Merge state abbreviations with census data
    census = census.merge(
        abbreviations[["State", "Other abbreviations"]],
        how="left",
        left_on="STNAME",
        right_on="State",
    )

    # Update County names to match shipment data
    census["County"] = census.loc[:, "CTYNAME"].apply(updateName)

    # Rename columns by year
    census = census.rename(
        columns={
            "POPESTIMATE2000": 2000,
            "POPESTIMATE2001": 2001,
            "POPESTIMATE2002": 2002,
            "POPESTIMATE2003": 2003,
            "POPESTIMATE2004": 2004,
            "POPESTIMATE2005": 2005,
            "POPESTIMATE2006": 2006,
            "POPESTIMATE2007": 2007,
            "POPESTIMATE2008": 2008,
            "POPESTIMATE2009": 2009,
            "CENSUS2010POP": 2010,
            "POPESTIMATE2011": 2011,
            "POPESTIMATE2012": 2012,
            "POPESTIMATE2013": 2013,
            "POPESTIMATE2014": 2014,
            "POPESTIMATE2015": 2015,
            "POPESTIMATE2016": 2016,
            "POPESTIMATE2017": 2017,
            "POPESTIMATE2018": 2018,
            "POPESTIMATE2019": 2019,
            "POPESTIMATE2020": 2020,
        }
    )

    # Pivot census data so years are rows instead of columns
    census = census.melt(
        id_vars=["STNAME", "CTYNAME", "State", "Other abbreviations", "County"],
        value_vars=[
            2000,
            2001,
            2002,
            2003,
            2004,
            2005,
            2006,
            2007,
            2008,
            2009,
            2010,
            2011,
            2012,
            2013,
            2014,
            2015,
            2016,
            2017,
            2018,
            2019,
            2020,
        ],
        var_name="Year",
        value_name="Population",
    )

    # Change population data to integer
    census["Population"] = census["Population"].astype(int)

    return census


# Update county names to match shipment data
def updateName(x):
    y = x.split()
    if y[-1] == "County":
        z = y[0]
        if len(y[-1]) > 1:
            for word in y[1:-1]:
                z += " " + word
        return z.upper()
    else:
        return x.upper()


# Calculate per capita opioid shipments
def perCapita(census, groupedShipments):
    mergedDF = pd.DataFrame()
    for year in range(2000, 2021):
        yearDF = groupedShipments.merge(
            census,
            how="left",
            left_on=["BUYER_STATE", "BUYER_COUNTY", "Year"],
            right_on=["Other abbreviations", "County", "Year"],
        )
        mergedDF = pd.concat([mergedDF, yearDF], axis=0)

    mergedDF["Opioids_per_Capita"] = (
        mergedDF["Converted Units"] / mergedDF["Population"]
    )

    return mergedDF


# Split data by test states and control states, both pre and post policy changes
def splitData(testState, controlStates, policyYear):
    test = mergedDF.loc[mergedDF.loc[:, "BUYER_STATE"] == testState, :]
    control = mergedDF.loc[mergedDF.loc[:, "BUYER_STATE"].isin(controlStates), :]
    pre_test = test.loc[test.loc[:, "Year"] < policyYear, :]
    post_test = test.loc[test.loc[:, "Year"] >= policyYear, :]
    pre_control = control.loc[control.loc[:, "Year"] < policyYear, :]
    post_control = control.loc[control.loc[:, "Year"] >= policyYear, :]
    pre_test_mean = pre_test.groupby(["BUYER_STATE", "Year"], as_index=False).mean(
        numeric_only=True
    )
    post_test_mean = post_test.groupby(["BUYER_STATE", "Year"], as_index=False).mean(
        numeric_only=True
    )
    pre_control_mean = pre_control.groupby(
        ["BUYER_STATE", "Year"], as_index=False
    ).mean(numeric_only=True)
    post_control_mean = post_control.groupby(
        ["BUYER_STATE", "Year"], as_index=False
    ).mean(numeric_only=True)
    return pre_test_mean, post_test_mean, pre_control_mean, post_control_mean


# Fit regression for plot
def get_reg_fit(data, yvar, xvar, alpha=0.05):
    import statsmodels.formula.api as smf

    # Grid for predicted values
    x = data.loc[pd.notnull(data[yvar]), xvar]
    xmin = x.min()
    xmax = x.max()
    step = (xmax - xmin) / 100
    grid = np.arange(xmin, xmax + step, step)
    predictions = pd.DataFrame({xvar: grid})

    # Fit model, get predictions
    model = smf.ols(f"{yvar} ~ {xvar}", data=data).fit()
    model_predict = model.get_prediction(predictions[xvar])
    predictions[yvar] = model_predict.summary_frame()["mean"]
    predictions[["ci_low", "ci_high"]] = model_predict.conf_int(alpha=alpha)

    # Build chart
    stateList = data.loc[:, "BUYER_STATE"].unique().tolist()
    stateListString = stateList[0]
    treatment = 1
    if len(stateList) > 1:
        treatment = 0.5
        for eachState in stateList[1:]:
            stateListString += ", " + eachState
    predictions["State"] = stateListString
    reg = (
        alt.Chart(predictions)
        .mark_line(opacity=treatment)
        .encode(
            x=xvar,
            y=alt.Y(yvar, axis=alt.Axis(title="Opioids per Capita")),
            color="State",
        )
        .properties(title="Average Opioids per Capita Shipped by States")
    )
    ci = (
        alt.Chart(predictions)
        .mark_errorband(opacity=treatment / 2)
        .encode(
            x=xvar,
            y=alt.Y("ci_low", title="Opioids per Capita"),
            y2="ci_high",
        )
    )
    chart = ci + reg
    return predictions, chart


# Plot regression
def plotRegression(testState, controlStates, policyYear):
    split = splitData(testState, controlStates, policyYear)
    pre_test_fit, pre_test_reg_chart = get_reg_fit(
        split[0],
        yvar="Opioids_per_Capita",
        xvar="Year",
        alpha=0.05,
    )
    post_test_fit, post_test_reg_chart = get_reg_fit(
        split[1],
        yvar="Opioids_per_Capita",
        xvar="Year",
        alpha=0.05,
    )
    pre_control_fit, pre_control_reg_chart = get_reg_fit(
        split[2],
        yvar="Opioids_per_Capita",
        xvar="Year",
        alpha=0.05,
    )
    post_control_fit, post_control_reg_chart = get_reg_fit(
        split[3],
        yvar="Opioids_per_Capita",
        xvar="Year",
        alpha=0.05,
    )
    rule = (
        alt.Chart(post_test_fit.loc[post_test_fit.loc[:, "Year"] == policyYear, :])
        .mark_rule(color="black")
        .encode(alt.X("Year:Q", title="Year"))
    )
    return (pre_test_reg_chart + rule + post_test_reg_chart), (
        pre_test_reg_chart
        + rule
        + post_test_reg_chart
        + pre_control_reg_chart
        + post_control_reg_chart
    )


if __name__ == "__main__":
    states = [
        "FL",
        "GA",
        "OH",
        "PA",
        "MD",
        "WI",
        "AZ",
        "WA",
        "MI",
        "NC",
        "MO",
        "VA",
        "MA",
    ]
    groupedShipments = mergeStates(states)
    census = censusData()
    mergedDF = perCapita(census, groupedShipments)
    prePostFL, diffDiffFL = plotRegression("FL", ["MI", "NC", "OH"], 2010)
    prePostWA, diffDiffWA = plotRegression("WA", ["AZ", "MO", "GA"], 2012)
    prePostFL.display()
    diffDiffFL.display()
    prePostWA.display()
    diffDiffWA.display()
