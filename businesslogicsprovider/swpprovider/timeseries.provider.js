import PurchaseQueries from "../../models/purchasequeries.model.js";
import OverViewMasterNew from "../../models/newoverview.model.js";
import PurchaseTimeSeries from "../../models/purchasetimeseris.model.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import SwitchQueries from "../../models/switch.model.js";
import TimeseriesCachedData from "../../models/timeseries.model.js";
import SWPTimeseries from "../../models/swptimeseries.model.js";
import { getPoolObj } from "../../utils/pool.js";
import SwpQueries from "../../models/swpqueries.model.js";

export const swp_timeseries_provider = async (
  startDate,
  endDate,
  fund,
  snsstatus = false
) => {
  try {
    const pool = getPoolObj(fund);
    let schema = getFundWiseSchema(fund);
    const isDataPresent = await TimeseriesCachedData.findOne({
      $and: [
        {
          fund: fund,
          startDate: startDate,
          endDate: endDate,
          type: "swp",
        },
      ],
    });
    if (!snsstatus && isDataPresent != null) {

      return isDataPresent.data;
    } else {
      const main = async () => {
        let formattedQueries = [];
        let queries = await SwpQueries.findOne(
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
            type: "swp",
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
    console.log("Error in swp timeseries provider", e);
    throw new Error(e);
  }
};
