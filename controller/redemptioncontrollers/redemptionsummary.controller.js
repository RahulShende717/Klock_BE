import { redemptionSummary } from "../../businesslogicsprovider/redemptionprovider/redemptionsummary.provider.js";

export const redemption_summary = async (req, res) => {
  try {
    let dataObject = {};
    let transaction_date = req.body.transaction_date;
    let fund = req.body.fund;
    const result = await redemptionSummary(transaction_date, fund);
    res.status(200).send(result);
  } catch (e) {
    // console.log("Error while fetching summary details redemotion,", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
