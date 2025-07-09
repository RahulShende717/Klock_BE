import { switchsummary_provider } from "../../businesslogicsprovider/switchprovider/switchsummary.provider.js";

export const switch_summary_controller = async (req, res) => {
  try {
    const { fund, transaction_date } = req.body;
    const data = await switchsummary_provider(transaction_date, fund);
    res.status(200).send(data);
  } catch (e) {
    // console.log("Error in purchase timeseris controller", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
