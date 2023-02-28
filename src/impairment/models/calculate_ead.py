import calendar
import tracdap.rt.api as trac
import typing as tp
from impairment import schemas as schemas
import datetime
import pandas as pd


class CalculateEad(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.declare_parameters(

            trac.P("first_forecast_month", trac.BasicType.DATE, label="First month of forecast", default_value=datetime.datetime(2022, 1, 1).date()),
            trac.P("last_forecast_month", trac.BasicType.DATE, label="Last month of forecast", default_value=datetime.datetime(2025, 12, 1).date())
        )

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        ead_model_parameters_schema = trac.load_schema(schemas, "ead_model_parameters_schema.csv")
        balance_forecast_schema = trac.load_schema(schemas, "balance_forecast_schema.csv")
        mortgage_book_t0_schema = trac.load_schema(schemas, "mortgage_book_t0_schema.csv")

        return {"ead_model_parameters": trac.ModelInputSchema(ead_model_parameters_schema),
                "balance_forecast": trac.ModelInputSchema(balance_forecast_schema),
                "mortgage_book_t0": trac.ModelInputSchema(mortgage_book_t0_schema)}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        ead_forecast_schema = trac.load_schema(schemas, "ead_forecast_schema.csv")

        return {"ead_forecast": trac.ModelOutputSchema(ead_forecast_schema)}

    def run_model(self, ctx: trac.TracContext):
        first_forecast_month = ctx.get_parameter("first_forecast_month")
        last_forecast_month = ctx.get_parameter("last_forecast_month")

        last_day = calendar.monthrange(first_forecast_month.year, first_forecast_month.month)[1]
        first_forecast_month = first_forecast_month.replace(day=last_day)

        last_day = calendar.monthrange(last_forecast_month.year, last_forecast_month.month)[1]
        last_forecast_month = last_forecast_month.replace(day=last_day)

        date_list = pd.date_range(first_forecast_month, last_forecast_month, freq="M", inclusive="both", name="date")
        dates = date_list.to_series("date").to_frame("date").reset_index(drop=True)

        # ead_model_parameters = ctx.get_pandas_table("ead_model_parameters")
        balance_forecast = ctx.get_pandas_table("balance_forecast")
        mortgage_book_t0 = ctx.get_pandas_table("mortgage_book_t0")

        ead_forecast = mortgage_book_t0.drop("date", axis=1).join(dates, how="cross")

        ead_forecast["time_to_default"] = 3 - ead_forecast["months_in_arrears"]

        ead_forecast["month_index"] = 1 + (ead_forecast["date"].dt.year - first_forecast_month.year) * 12 + ead_forecast["date"].dt.month - first_forecast_month.month
        ead_forecast["balance"] = ead_forecast["balance"] - ead_forecast["month_index"] * ead_forecast["monthly_repayment"]

        ead_forecast["ead"] = ead_forecast["balance"] + 400 + ead_forecast["balance"] * (
                    pow(1 + (0.05 / 12), ead_forecast["time_to_default"]) - 1)

        ead_forecast.drop(["pd_12m", "in_default", "month_index"], axis=1, inplace=True)
        # Output the dataset
        ctx.put_pandas_table("ead_forecast", ead_forecast)


if __name__ == "__main__":
    import tracdap.rt.launch as launch

    launch.launch_model(CalculateEad, "config/impairment/calculate_ead.yaml", "config/sys_config.yaml")
