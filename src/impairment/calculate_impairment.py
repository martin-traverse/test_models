import tracdap.rt.api as trac
import typing as tp
from impairment import schemas as schemas


class CalculateImpairment(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.define_parameters(
            trac.P("impairment_weight", trac.FLOAT, "Apply an impairment weight", default_value=1.0)
        )

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        pd_forecast_schema = trac.load_schema(schemas, "pd_forecast_schema.csv")
        lgd_forecast_schema = trac.load_schema(schemas, "lgd_forecast_schema.csv")

        return {"pd_forecast": trac.ModelInputSchema(pd_forecast_schema),
                "lgd_forecast": trac.ModelInputSchema(lgd_forecast_schema)}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        impairment_forecast_schema = trac.load_schema(schemas, "impairment_forecast_schema.csv")

        return {"impairment_forecast": trac.ModelOutputSchema(impairment_forecast_schema)}

    def run_model(self, ctx: trac.TracContext):

        impairment_weight = ctx.get_parameter("impairment_weight")

        pd_forecast = ctx.get_pandas_table("pd_forecast")
        lgd_forecast = ctx.get_pandas_table("lgd_forecast")

        impairment_forecast = lgd_forecast.merge(pd_forecast[["id", "date", "pd_12m", "pd_lifetime"]], on=['id', "date"])
        impairment_forecast["ecl_12m"] = impairment_forecast["ead"] * impairment_forecast["pd_12m"] * impairment_forecast["lgd_12m"] * impairment_weight
        impairment_forecast["ecl_lifetime"] = impairment_forecast["ead"] * impairment_forecast["pd_lifetime"] * impairment_forecast["lgd_lifetime"] * impairment_weight

        # Output the dataset
        ctx.put_pandas_table("impairment_forecast", impairment_forecast)


if __name__ == "__main__":
    import tracdap.rt.launch as launch
    launch.launch_model(CalculateImpairment, "config/impairment/calculate_impairment.yaml", "config/sys_config.yaml")
