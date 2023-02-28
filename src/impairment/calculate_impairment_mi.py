import tracdap.rt.api as trac
import typing as tp
from impairment import schemas as schemas


class CalculateImpairmentMI(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.declare_parameters()

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        impairment_forecast_schema = trac.load_schema(schemas, "impairment_forecast_schema.csv")

        return {"impairment_forecast": trac.ModelInputSchema(impairment_forecast_schema)}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        impairment_mi_schema = trac.load_schema(schemas, "impairment_mi_schema.csv")

        return {"impairment_mi": trac.ModelOutputSchema(impairment_mi_schema)}

    def run_model(self, ctx: trac.TracContext):
        impairment_forecast = ctx.get_pandas_table("impairment_forecast")

        group_by_list = ['business_line', "mortgage_type", "date"]
        impairment_mi = (impairment_forecast.groupby(group_by_list, as_index=False)
              .agg({'balance': 'sum', 'ead': 'sum', 'pd_12m': 'mean', 'pd_lifetime': 'mean', 'lgd_12m': 'mean', 'lgd_lifetime': 'mean', 'ecl_12m': 'sum', 'ecl_lifetime': 'sum'})
              .rename(columns={'balance': 'total_balance', 'ead': 'total_ead', 'pd_12m': 'mean_pd_12m', 'pd_lifetime': 'mean_pd_lifetime', 'lgd_12m': 'mean_lgd_12m', 'lgd_lifetime': 'mean_lgd_lifetime', 'ecl_12m': 'total_ecl_12m', 'ecl_lifetime': 'total_ecl_lifetime'}))

        impairment_mi = impairment_mi.sort_values(by=group_by_list, ascending=True)

        # Output the dataset
        ctx.put_pandas_table("impairment_mi", impairment_mi)


if __name__ == "__main__":
    import tracdap.rt.launch as launch

    launch.launch_model(CalculateImpairmentMI, "config/impairment/calculate_impairment_mi.yaml", "config/sys_config.yaml")
