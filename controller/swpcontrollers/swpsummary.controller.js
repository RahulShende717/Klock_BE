import { funddetails_nigo_platform_summary_provider } from "../../businesslogicsprovider/swpprovider/funddetailsnigoplatformsummary.provider.js";
import { swp_summary_provider } from "../../businesslogicsprovider/swpprovider/swpsummary.provider.js";

export const swp_summary_controller = async (req, res) => {
  try {
    const transaction_date = req.body.transaction_date;
    const fund = req.body.fund;
    const data = await swp_summary_provider(transaction_date, fund);
    res.status(200).send(data);
  } catch (e) {
    // console.log("Error while fetchinh data for overview", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
