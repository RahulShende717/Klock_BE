import mongoose from "mongoose";

export const handle_cache_logs = async (req, res) => {
  try {
    let sevenDayflag = await mongoose.connection
      .collection("weeklyFlag")
      .find({})
      .toArray();
    res.status(200).send(sevenDayflag);
  } catch (error) {
    console.log("error in sevenDayflag Recoards", error);
  }
};
