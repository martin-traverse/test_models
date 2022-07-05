#  Copyright 2020 Accenture Global Solutions Limited
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License attt
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
import pandas as pd
import numpy as np
import datetime

# Set display options
pd.set_option("display.max.columns", None)

class CalculateEad(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.declare_parameters(
            trac.P("advanced_logging", trac.BasicType.BOOLEAN, label="Advanced logging")
        )

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        ead_model_parameters = trac.declare_input_table(
            trac.F("ID", trac.BasicType.STRING, label="Parameter ID"),
            trac.F("DESCRIPTION", trac.BasicType.STRING, label="Parameter description"),
            trac.F("VALUE", trac.BasicType.FLOAT, label="Parameter value")
        )
        
        account_pd2 = trac.declare_input_table(
            trac.F("OBSERVATION_DATE", trac.BasicType.DATE, label="Date", format_code="MONTH"),
            trac.F("BALANCE", trac.BasicType.FLOAT, label="Balance (drawn)", format_code=",|.|2|£|"),
            trac.F("PD", trac.BasicType.FLOAT, label="Probability of default", format_code="|.|4||%")
        )

        return {"ead_model_parameters": ead_model_parameters, "account_pd2": account_pd2}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        account_ead = trac.declare_output_table(
            trac.F("OBSERVATION_DATE", trac.BasicType.DATE, label="Date", business_key=True, categorical=False, format_code="MONTH"),
            trac.F("BALANCE", trac.BasicType.FLOAT, label="Balance (drawn)", format_code=",|.|2|£|"),
            trac.F("PD", trac.BasicType.FLOAT, label="Probability of default", format_code="|.|4||%"),
            trac.F("EAD", trac.BasicType.FLOAT, label="EAD", format_code="|.|0|£|")
        )

        return {"account_ead": account_ead}

    def run_model(self, ctx: trac.TracContext):
      
        ead_model_parameters = ctx.get_pandas_table("ead_model_parameters")
        account_pd2 = ctx.get_pandas_table("account_pd2")
        
        account_ead = account_pd2.copy()
        
        account_ead["EAD"] = account_ead["BALANCE"] * 1.05
        
        # Output the dataset
        ctx.put_pandas_table("account_ead", account_ead)
      
if __name__ == "__main__":
    import trac.rt.launch as launch
    launch.launch_model(CalculateEad, "calculate_ead.yaml", "../sys_config.yaml")
