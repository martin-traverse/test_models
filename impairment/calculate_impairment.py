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
import pandas as pd
import numpy as np
import datetime

# Set display options again
pd.set_option("display.max.columns", None)

class CalculateImpairment(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.declare_parameters()
    
    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        
        account_lgd = trac.declare_input_table(
            trac.F("OBSERVATION_DATE", trac.BasicType.DATE, label="Date", business_key=True, categorical=False, format_code="MONTH"),
            trac.F("BALANCE", trac.BasicType.FLOAT, label="Balance (drawn)", format_code=",|.|2|£|"),
            trac.F("PD", trac.BasicType.FLOAT, label="Probability of default", format_code="|.|4||%"),
            trac.F("EAD", trac.BasicType.FLOAT, label="EAD", format_code="|.|0|£|"),
            trac.F("LGD", trac.BasicType.FLOAT, label="Loss given default", format_code="|.|2||%")
        )

        return {"account_lgd": account_lgd}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        account_impairment = trac.declare_output_table(
            trac.F("OBSERVATION_DATE", trac.BasicType.DATE, label="Date", business_key=True, categorical=False, format_code="MONTH"),
            trac.F("BALANCE", trac.BasicType.FLOAT, label="Balance (drawn)", format_code=",|.|2|£|"),
            trac.F("PD", trac.BasicType.FLOAT, label="Probability of default", format_code="|.|4||%"),
            trac.F("EAD", trac.BasicType.FLOAT, label="EAD", format_code="|.|0|£|"),
            trac.F("LGD", trac.BasicType.FLOAT, label="Loss given default", format_code="|.|2||%"),
            trac.F("IMPAIRMENT", trac.BasicType.FLOAT, label="Impairment (forward look)", format_code="|.|2||%")
        )

        return {"account_impairment": account_impairment}

    def run_model(self, ctx: trac.TracContext):
      
        account_lgd = ctx.get_pandas_table("account_lgd")
        
        account_impairment = account_lgd.copy()
        
        account_impairment["IMPAIRMENT"] = account_impairment["PD"] * account_impairment["EAD"] * account_impairment["LGD"]
        
        # Output the dataset
        ctx.put_pandas_table("account_impairment", account_impairment)
      
if __name__ == "__main__":
    import trac.rt.launch as launch
    launch.launch_model(CalculateImpairment, "calculate_impairment.yaml", "../sys_config.yaml")
