import { getFundWiseSchema } from "../utils/schemamapping.js";
// import { getPoolByFund } from "../lib/postgressConnect.js";
import { getPoolObj } from "../utils/pool.js";
import TrjourneycachedData from "../models/trjourneycacheddata.model.js";
import TrJourneyQueries from "../models/trjourneyqueries.model.js";

export const cache_transaction_journey_provider = async (
  rows,
  fund,
  transaction_date,
  trtype,
  asset_class
) => {
  try {
    let schema = getFundWiseSchema(fund);
    const pool = getPoolObj(fund);

    if (rows.length > 0) {
      const results = await TrJourneyQueries.findOne({ type: trtype });
      const execution_query = results.query;
      let formattedQuery = execution_query.replace(/schema/g, `${schema}`);
      formattedQuery = formattedQuery.replace(
        /(ihnoarray)/g,
        `${rows.map((field) => `'${field}'`).join(",")}`
      );
      const result = await pool.query(formattedQuery);
      const cached_data = await TrjourneycachedData.findOneAndUpdate(
        { fund: fund, date: transaction_date, type: trtype },
        {
          data: result.rows,
        },
        {
          upsert: true,
          new: true,
        }
      );
    }

    // if (result.rows.length > 1) {
    //   for (let i = 0; i < result.rows.length; i++) {
    //     await cachedata(result.rows[i], fund, transaction_date);
    //   }
    // } else {
    //   await cachedata(result.rows[0], fund, transaction_date);
    // }
    // let obj = result.rows;
  } catch (e) {
    console.log("Error in ihno provider", e);
  }
};
