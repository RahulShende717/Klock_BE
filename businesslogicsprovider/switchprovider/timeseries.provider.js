import PurchaseQueries from "../../models/purchasequeries.model.js";
import OverViewMasterNew from "../../models/newoverview.model.js";
import PurchaseTimeSeries from "../../models/purchasetimeseris.model.js";
import SwitchQueries from "../../models/switch.model.js";
import TimeseriesCachedData from "../../models/timeseries.model.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import { getPoolObj } from "../../utils/pool.js";

export const switch_timeseries_provider = async (
  startdate,
  enddate,
  fundcode,
  snsstatus = false
) => {
  try {
    const fund = fundcode,
      startDate = startdate,
      endDate = enddate;
    let schema = getFundWiseSchema(fund);
    const pool = getPoolObj(fund);
    const isDataPresent = await TimeseriesCachedData.findOne({
      $and: [
        {
          fund: fund,
          startDate: startDate,
          endDate: endDate,
          type: "switch",
        },
      ],
    });
    if (!snsstatus && isDataPresent != null) {

      return isDataPresent.data;
    } else {
      const main = async () => {
        let formattedQueries = [];
        let queries = await SwitchQueries.findOne(
          { endpoint: "Timeseries" },
          { queriesArray: 1 }
        );
        let queriesArray = queries.queriesArray;
        queriesArray.map((ele) => {
          let formattedQuery = ele.query.replace(
            /'startDate'/g,
            `'${startDate}'`
          );
          formattedQuery = formattedQuery.replace(/startDate/g, `${startDate}`);
          formattedQuery = formattedQuery.replace(/schema/g, `${schema}`);
          formattedQuery = formattedQuery.replace(/'fund'/g, `'${fund}'`);
          formattedQuery = formattedQuery.replace(/'endDate'/g, `'${endDate}'`);
          formattedQuery = formattedQuery.replace(/endDate/g, `${endDate}`);
          formattedQueries.push({ name: ele.name, query: formattedQuery });
        });
        
        const result = await pool.query(formattedQueries[0].query);
        const liquid_array = result.rows.filter(
          (ele, ind) => ele.asset_class == "LIQUID"
        );
        const nonliquid_array = result.rows.filter(
          (ele, ind) => ele.asset_class == "NON LIQUID"
        );
        let obj = {
          combine: result.rows,
          liquid: liquid_array,
          nonliquid: nonliquid_array,
        };

        const cached_data = await TimeseriesCachedData.findOneAndUpdate(
          {
            fund: fund,
            startDate: startDate,
            endDate: endDate,
            type: "switch",
          },
          {
            $set: {
              fund: fund,
              startDate: startDate,
              endDate: endDate,
              data: obj,
            },
          },
          {
            upsert: true,
            new: true,
          }
        );
        return cached_data.data;
      };

      const obj = await main();
      return obj;
    }
  } catch (e) {
    console.log("Error in switch timeseris controller", e);
    throw new Error(e);
  }
};
