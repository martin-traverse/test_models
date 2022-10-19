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


class CalculateMI(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.declare_parameters()

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        impairment_forecast_schema = trac.load_schema(schemas, "impairment_forecast.csv")

        return {"impairment_forecast": trac.ModelInputSchema(impairment_forecast_schema)}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        impairment_mi_schema = trac.load_schema(schemas, "impairment_mi.csv")

        return {"impairment_mi": trac.ModelOutputSchema(impairment_mi_schema)}

    def run_model(self, ctx: trac.TracContext):
        impairment_forecast = ctx.get_pandas_table("impairment_forecast")

        impairment_mi = (impairment_forecast.groupby(['business_line', "product_line", "region", "subregion", "mortgage_type"], as_index=False)
              .agg({'balance': 'sum', 'ead': 'sum', 'pd_12m': 'mean', 'pd_lifetime': 'mean', 'lgd_12m': 'mean', 'lgd_lifetime': 'mean', 'ecl_12m': 'sum', 'ecl_lifetime': 'sum'})
              .rename(columns={'balance': 'total_balance', 'ead': 'total_ead', 'pd_12m': 'mean_pd_12m', 'pd_lifetime': 'mean_pd_lifetime', 'lgd_12m': 'mean_lgd_12m', 'lgd_lifetime': 'mean_lgd_lifetime', 'ecl_12m': 'total_ecl_12m', 'ecl_lifetime': 'total_ecl_lifetime'}))

        # Output the dataset
        ctx.put_pandas_table("impairment_mi", impairment_mi)


if __name__ == "__main__":
    import tracdap.rt.launch as launch

    launch.launch_model(CalculateMI, "config/calculate_mi.yaml", "config/sys_config.yaml")
