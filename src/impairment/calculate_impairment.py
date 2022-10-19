#  Copyright 2020 Accenture Global Solutions Limited
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import tracdap.rt.api as trac
import typing as tp
import impairment.schemas as schemas


class CalculateImpairment(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.declare_parameters()

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        pd_forecast_schema = trac.load_schema(schemas, "pd_forecast.csv")
        lgd_forecast_schema = trac.load_schema(schemas, "lgd_forecast.csv")

        return {"pd_forecast": trac.ModelInputSchema(pd_forecast_schema),
                "lgd_forecast": trac.ModelInputSchema(lgd_forecast_schema)}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        impairment_forecast_schema = trac.load_schema(schemas, "impairment_forecast.csv")

        return {"impairment_forecast": trac.ModelOutputSchema(impairment_forecast_schema)}

    def run_model(self, ctx: trac.TracContext):
        pd_forecast = ctx.get_pandas_table("pd_forecast")
        lgd_forecast = ctx.get_pandas_table("lgd_forecast")

        impairment_forecast = lgd_forecast.merge(pd_forecast[["id", "pd_12m", "pd_lifetime"]], on='id')
        impairment_forecast["ecl_12m"] =   impairment_forecast["ead"] * impairment_forecast["pd_12m"]* impairment_forecast["lgd_12m"]
        impairment_forecast["ecl_lifetime"] = impairment_forecast["ead"] * impairment_forecast["pd_lifetime"] * \
                                         impairment_forecast["lgd_lifetime"]

        # Output the dataset
        ctx.put_pandas_table("impairment_forecast", impairment_forecast)


if __name__ == "__main__":
    import tracdap.rt.launch as launch

    launch.launch_model(CalculateImpairment, "config/calculate_impairment.yaml", "config/sys_config.yaml")
