import { transaction_history_provider_swp } from "../../businesslogicsprovider/swpprovider/swtransactions.provider.js";

export const trxn_history_controller = async (req, res) => {
  try {
    const transaction_date = req.body.transaction_date;
    const fund = req.body.fund;
    const data = await transaction_history_provider_swp(transaction_date, fund);
    res.status(200).send(data);
  } catch (e) {
    // console.log("Error while fetchinh data for overview", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
