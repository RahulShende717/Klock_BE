import { switch_timeseries_provider } from "../../businesslogicsprovider/switchprovider/timeseries.provider.js";

export const timeseries_controller = async (req, res) => {
  try {
    const { fund, startDate, endDate } = req.body;
    const data = await switch_timeseries_provider(startDate, endDate, fund);
    res.status(200).send(data);
  } catch (e) {
    // console.log("Error in purchase timeseris controller", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
