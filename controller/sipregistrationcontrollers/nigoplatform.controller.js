import { nigo_platform_provider } from "../../businesslogicsprovider/sipregistrationprovider/nigoplatform.provider.js";
import { registration_summary_provider } from "../../businesslogicsprovider/sipregistrationprovider/registartionsummary.provider.js";

export const nigo_platform_controller = async (req, res) => {
  try {
    const transaction_date = req.body.transaction_date;
    const fund = req.body.fund;
    const data = await nigo_platform_provider(transaction_date, fund);
    res.status(200).send(data);
  } catch (e) {
    // console.log("Error while fetchinh data for overview", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
