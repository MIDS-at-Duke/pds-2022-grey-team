"""Death of opioid overdose analysis"""

# Read merged pop and deaths dataset
import statsmodels.formula.api as smf
import pandas as pd
import altair as alt
import numpy as np

df = pd.read_csv(
    "https://raw.githubusercontent.com/MIDS-at-Duke/pds-2022-grey-team/main/00_Source/merged_pop_drug_death.csv"
)

# build regression model
def reg_fit(data, color, yvar, xvar, legend, alpha=0.05):
    colour = color
    years = list(np.arange(2003, 2016, 1))
    x = data.loc[pd.notnull(data[yvar]), xvar]
    xmin = x.min()
    xmax = x.max()
    step = (xmax - xmin) / 100
    grid = np.arange(xmin, xmax + step, step)
    predictions = pd.DataFrame({xvar: grid})

    # Fit model
    model = smf.ols(f"{yvar} ~ {xvar}", data=data).fit()
    model_predict = model.get_prediction(predictions[xvar])
    predictions[yvar] = model_predict.summary_frame()["mean"]
    predictions[["ci_low", "ci_high"]] = model_predict.conf_int(alpha=alpha)

    # Build chart
    predictions["Treat"] = f"{legend}"
    reg = (
        alt.Chart(predictions)
        .mark_line()
        .encode(
            x=xvar,
            y=alt.Y(yvar),
            color=alt.value(f"{colour}"),
            opacity=alt.Opacity("Treat", legend=alt.Legend(title="State")),
        )
    )
    ci = (
        alt.Chart(predictions)
        .mark_errorband()
        .encode(
            alt.X(f"{xvar}:Q", axis=alt.Axis(format=".0f", values=years)),
            y=alt.Y(
                "ci_low",
                title="Mortality Ratio per 100000 due to Opioid Overdose (by County)",
                scale=alt.Scale(zero=False),
            ),
            y2="ci_high",
            color=alt.value(f"{colour}"),
        )
    )
    chart = ci + reg
    return predictions, chart


# plot chart
def plotting_chart(policy_year, color, data, yvar, xvar, legend, alpha=0.05):
    pl_year = policy_year
    pol_year = []
    pol_year.append(int(pl_year))
    years = list(np.arange(2003, 2016, 1))

    # Plot chart
    fit, reg_chart = reg_fit(
        color=color, data=data, yvar=yvar, xvar=xvar, legend=legend, alpha=alpha
    )
    policy = pd.DataFrame({"Year": pol_year})

    rule = (
        alt.Chart(policy)
        .mark_rule(color="black")
        .encode(alt.X("Year:Q", title="Year", axis=alt.Axis(values=years)))
    )
    return (reg_chart + rule).properties(width=500, height=500)


# split FL data via year pre-policy and post-policy
data_FL = df.loc[df["State"] == "FL", ["Year", "County", "Death_Ratio_per_100000"]]
fl_pre = data_FL[data_FL["Year"] < 2010]
fl_post = data_FL[data_FL["Year"] >= 2010]

# FL plot
pre_fl_fit = plotting_chart(
    2010, "blue", fl_pre, "Death_Ratio_per_100000", "Year", legend="Florida", alpha=0.05
)
post_fl_fit = plotting_chart(
    2010,
    "blue",
    fl_post,
    "Death_Ratio_per_100000",
    "Year",
    legend="Florida",
    alpha=0.05,
)
final = pre_fl_fit + post_fl_fit

final.properties(
    title="Pre-Policy VS. Post-Policy of Opioid Regulations on Mortality Ratio for Florida"
)

# FL and its control states
diff_FL = df.loc[df["State"].isin(["FL", "MI", "NC", "OH"])].copy()
diff_FL["Treat"] = 1
diff_FL.loc[diff_FL["State"].isin(["MI", "NC", "OH"]), "Treat"] = 0
diff_FL_treat = diff_FL.loc[diff_FL["Treat"] == 1]
diff_FL_control = diff_FL.loc[diff_FL["Treat"] == 0]
diff_FL_treat_pre = diff_FL_treat.loc[diff_FL_treat["Year"] < 2010]
diff_FL_treat_post = diff_FL_treat.loc[diff_FL_treat["Year"] >= 2010]
diff_FL_control_pre = diff_FL_control.loc[diff_FL_control["Year"] < 2010]
diff_FL_control_post = diff_FL_control.loc[diff_FL_control["Year"] >= 2010]

pre_FL = plotting_chart(
    2010,
    "blue",
    diff_FL_treat_pre,
    "Death_Ratio_per_100000",
    "Year",
    "Florida",
    alpha=0.05,
)
post_FL = plotting_chart(
    2010,
    "blue",
    diff_FL_treat_post,
    "Death_Ratio_per_100000",
    "Year",
    "Florida",
    alpha=0.05,
)
pre_control = plotting_chart(
    2010,
    "#456bd6",
    diff_FL_control_pre,
    "Death_Ratio_per_100000",
    "Year",
    "Comparison States - MI, NC, OH",
    alpha=0.05,
)
post_control = plotting_chart(
    2010,
    "#456bd6",
    diff_FL_control_post,
    "Death_Ratio_per_100000",
    "Year",
    "Comparison States - MI, NC, OH",
    alpha=0.05,
)

# FL vs MI, NC, OH
final = pre_FL + post_FL + pre_control + post_control
final.properties(
    title="Difference in Difference Analysis of Opioid Regulations on Mortality Ratio for Florida vs Comparison States"
)

# split TX data via year pre-policy and post-policy
data_TX = df.loc[df["State"] == "TX", ["Year", "County", "Death_Ratio_per_100000"]]
tx_pre = data_TX[data_TX["Year"] < 2007]
tx_post = data_TX[data_TX["Year"] >= 2007]

# TX plot
pre_tx_fit = plotting_chart(
    2007, "green", tx_pre, "Death_Ratio_per_100000", "Year", legend="Texas", alpha=0.05
)
post_tx_fit = plotting_chart(
    2007, "green", tx_post, "Death_Ratio_per_100000", "Year", legend="Texas", alpha=0.05
)
final = pre_tx_fit + post_tx_fit

final.properties(
    title="Pre-Policy VS. Post-Policy of Opioid Regulations on Mortality Ratio for Texas"
)

# TX and its control states
diff_TX = df.loc[df["State"].isin(["TX", "PA", "MA", "VA"])].copy()
diff_TX["Treat"] = 1
diff_TX.loc[diff_TX["State"].isin(["PA", "MA", "VA"]), "Treat"] = 0
diff_TX_treat = diff_TX.loc[diff_TX["Treat"] == 1]
diff_TX_control = diff_TX.loc[diff_TX["Treat"] == 0]
diff_TX_treat_pre = diff_TX_treat.loc[diff_TX_treat["Year"] < 2007]
diff_TX_treat_post = diff_TX_treat.loc[diff_TX_treat["Year"] >= 2007]
diff_TX_control_pre = diff_TX_control.loc[diff_TX_control["Year"] < 2007]
diff_TX_control_post = diff_TX_control.loc[diff_TX_control["Year"] >= 2007]

pre_TX = plotting_chart(
    2007,
    "green",
    diff_TX_treat_pre,
    "Death_Ratio_per_100000",
    "Year",
    "Texas",
    alpha=0.05,
)
post_TX = plotting_chart(
    2007,
    "green",
    diff_TX_treat_post,
    "Death_Ratio_per_100000",
    "Year",
    "Texas",
    alpha=0.05,
)
pre_control = plotting_chart(
    2007,
    "#6FCC49",
    diff_TX_control_pre,
    "Death_Ratio_per_100000",
    "Year",
    "Comparison States - PA, MA, VA",
    alpha=0.05,
)
post_control = plotting_chart(
    2007,
    "#6FCC49",
    diff_TX_control_post,
    "Death_Ratio_per_100000",
    "Year",
    "Comparison States - PA, MA, VA",
    alpha=0.05,
)

# TX vs PA, MA, VA
final = pre_TX + post_TX + pre_control + post_control
final.properties(
    title="Difference in Difference Analysis of Opioid Regulations on Mortality Ratio for Texas vs Comparison States"
)

# split WA data via year pre-policy and post-policy
data_WA = df.loc[df["State"] == "WA", ["Year", "County", "Death_Ratio_per_100000"]]
wa_pre = data_WA[data_WA["Year"] < 2012]
wa_post = data_WA[data_WA["Year"] >= 2012]

# WA plot
pre_wa_fit = plotting_chart(
    2012,
    "brown",
    wa_pre,
    "Death_Ratio_per_100000",
    "Year",
    legend="Washington",
    alpha=0.05,
)
post_wa_fit = plotting_chart(
    2012,
    "brown",
    wa_post,
    "Death_Ratio_per_100000",
    "Year",
    legend="Washington",
    alpha=0.05,
)
final = pre_wa_fit + post_wa_fit

final.properties(
    title="Pre-Policy VS. Post-Policy of Opioid Regulations on Mortality Ratio for Washington"
)

# WA and its control states
diff_WA = df.loc[df["State"].isin(["WA", "AZ", "MO", "GA"])].copy()
diff_WA["Treat"] = 1
diff_WA.loc[diff_WA["State"].isin(["AZ", "MO", "GA"]), "Treat"] = 0
diff_WA_treat = diff_WA.loc[diff_WA["Treat"] == 1]
diff_WA_control = diff_WA.loc[diff_WA["Treat"] == 0]
diff_WA_treat_pre = diff_WA_treat.loc[diff_WA_treat["Year"] < 2012]
diff_WA_treat_post = diff_WA_treat.loc[diff_WA_treat["Year"] >= 2012]
diff_WA_control_pre = diff_WA_control.loc[diff_WA_control["Year"] < 2012]
diff_WA_control_post = diff_WA_control.loc[diff_WA_control["Year"] >= 2012]

pre_WA = plotting_chart(
    2012,
    "brown",
    diff_WA_treat_pre,
    "Death_Ratio_per_100000",
    "Year",
    "Washington",
    alpha=0.05,
)
post_WA = plotting_chart(
    2012,
    "brown",
    diff_WA_treat_post,
    "Death_Ratio_per_100000",
    "Year",
    "Washington",
    alpha=0.05,
)
pre_control = plotting_chart(
    2012,
    "#D4879E",
    diff_WA_control_pre,
    "Death_Ratio_per_100000",
    "Year",
    "Comparison States - AZ, MO, GA",
    alpha=0.05,
)
post_control = plotting_chart(
    2012,
    "#D4879E",
    diff_WA_control_post,
    "Death_Ratio_per_100000",
    "Year",
    "Comparison States - AZ, MO, GA",
    alpha=0.05,
)

# WA vs AZ, MO, GA
final = pre_WA + post_WA + pre_control + post_control
final.properties(
    title="Difference in Difference Analysis of Opioid Regulations on Mortality Ratio for Washington vs Comparison States"
)
