import { platform_nigo_summary_provider } from "../../businesslogicsprovider/switchprovider/platformwisenigosummary.provider.js";
import { switchalerts_verificationstatus } from "../../businesslogicsprovider/switchprovider/switchalertsverificationstatus.provider.js";

export const switchalerts_verificationstatus_controller = async (req, res) => {
  try {
    const { fund, transaction_date } = req.body;
    const data = await switchalerts_verificationstatus(transaction_date, fund);
    res.status(200).send(data);
  } catch (e) {
    // console.log("Error in purchase timeseris controller", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
