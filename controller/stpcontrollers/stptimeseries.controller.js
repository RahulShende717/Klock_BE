import { STPTimeSeriesData } from "../../businesslogicsprovider/stpprovider/stptimeseries.provider.js";

export const timeseries_controller = async (req, res) => {
  try {
    const { fund, startDate, endDate } = req.body;
    const data = await STPTimeSeriesData(startDate, endDate, fund);
    res.status(200).send(data);
  } catch (e) {
    res.status(500).send({ message: "Internal Server Error" });
    // console.log("Error in stp timeseris controller", e)
  }
};
