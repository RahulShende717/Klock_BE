import { funddetails_nigo_platform_summary_provider } from "../../businesslogicsprovider/swpprovider/funddetailsnigoplatformsummary.provider.js";

export const funddetailsnigoplatformsummaryController = async (req, res) => {
  try {
    const transaction_date = req.body.transaction_date;
    const fund = req.body.fund;
    const data = await funddetails_nigo_platform_summary_provider(
      transaction_date,
      fund
    );
    res.status(200).send(data);
  } catch (e) {
    // console.log("Error while fetchinh data for overview", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
