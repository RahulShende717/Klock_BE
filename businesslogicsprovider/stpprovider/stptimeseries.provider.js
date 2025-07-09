import { getPoolByFund } from "../../lib/postgressConnect.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import STPQueries from "../../models/stpqueries.model.js";
import TimeseriesCachedData from "../../models/timeseries.model.js";
import { getPoolObj } from "../../utils/pool.js";

export const STPTimeSeriesData = async (
  startDate,
  endDate,
  fundcode,
  snsstatus = false
) => {
  try {
    let fund = fundcode;
    let schema = getFundWiseSchema(fund);
    const pool = getPoolObj(fund);
    const isDataPresent = await TimeseriesCachedData.findOne({
      $and: [
        {
          fund: fund,
          startDate: startDate,
          endDate: endDate,
          type: "stp",
        },
      ],
    });
    if (!snsstatus && isDataPresent != null) {

      return isDataPresent.data;
    } else {
      const main = async () => {
        let formattedQueries = [];
        let purchesQueries = await STPQueries.findOne(
          { endpoint: "Timeseries" },
          { queriesArray: 1 }
        );
        let queriesArray = purchesQueries.queriesArray;
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
            type: "stp",
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
    console.log("Error in siptimeseries", e);
    throw new Error(e);
  }
};
