import { SIPSummearyData } from "../../businesslogicsprovider/sipprovider/sipsummary.provider.js";

export const SIP_summary_controller = async (req, res) => {
  try {
    const transaction_date = req.body.transaction_date;
    const fund = req.body.fund;
    const data = await SIPSummearyData(transaction_date, fund);
    res.status(200).send(data);
  } catch (e) {
    // console.log("Error while fetchinh data for overview", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
