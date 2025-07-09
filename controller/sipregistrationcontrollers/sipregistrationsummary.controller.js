import { registration_summary_provider } from "../../businesslogicsprovider/sipregistrationprovider/registartionsummary.provider.js";

export const registration_summary_controller = async (req, res) => {
  try {
    const transaction_date = req.body.transaction_date;
    const fund = req.body.fund;
    const data = await registration_summary_provider(transaction_date, fund);
    res.status(200).send(data);
  } catch (e) {
    // console.log("Error while fetchinh data for overview", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
