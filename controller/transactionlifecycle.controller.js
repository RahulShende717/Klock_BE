import TrJourneyQueries from "../models/trjourneyqueries.model.js";
import TrjourneycachedData from "../models/trjourneycacheddata.model.js";
import { getPoolObj } from "../utils/pool.js";
import { getFundWiseSchema } from "../utils/schemamapping.js";

export const get_transaction_lifecycle_details = async (req, res) => {
  let { ihno, currentDate, fund, trtype, externalclient , trdate} = req.body;
   console.log("Request Body", req.body.externalclient);
  let schema = getFundWiseSchema(fund);
  const pool = getPoolObj(fund);
  if (pool == null) console.log("No Pool Connections For This AMC", fund);

  try {
    let result =[];
    if(!externalclient){
    result = await TrjourneycachedData.aggregate([
      {
        $match: {
          date: currentDate,
          fund: fund,
          type: trtype,
        },
      },
      {
        $project: {
          data: {
            $filter: {
              input: "$data",
              as: "transaction",
              cond: { $eq: ["$$transaction.ihno", ihno] },
            },
          },
        },
      },
    ]);
    }
  
    if (result.length == 0 || result[0].data.length == 0 ) {
      //if in case ihno is not cached
      console.log("No Cached Data Found for this IHNO", { type: trtype , ...(externalclient !== undefined && { externalclient })});
      const result = await TrJourneyQueries.findOne({ type: trtype , ...(externalclient !== undefined && { externalclient })})
      // console.log("result", result);
      const execution_query = result.query;
      let formattedQuery = execution_query.replace(/schema/g, `${schema}`);
      formattedQuery = formattedQuery.replace(/(ihnoarray)/g, `'${ihno}'`);
      formattedQuery = formattedQuery.replace(/transactionDate/g, `${trdate}`);
      console.log("Formatted Query", formattedQuery);
      const response = await pool.query(formattedQuery);
      
 
      return res.status(200).send({ success: true, IHNO_Details: response.rows });
    } else {
      return res.status(200).send({ success: true, IHNO_Details: result[0].data });
    }
  } catch (error) {
    console.log("error", error);
    res.status(500).send({ message: "Internal Server Error" });
  }
};

