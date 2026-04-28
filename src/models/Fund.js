import mongoose from "mongoose";

const FundSchema = new mongoose.Schema(
  {
    schemeCode: { type: String, required: true, unique: true, index: true, trim: true },
    schemeName: { type: String, required: true, trim: true },
    nav: { type: Number, required: true },
    isin: { type: String, default: "", trim: true },
    navDate: { type: Date, required: true },
    lastUpdated: { type: Date, required: true },
    source: { type: String, default: "amfi" }
  },
  {
    timestamps: true,
    versionKey: false
  }
);

FundSchema.index({ schemeName: "text" });

export const Fund = mongoose.models.Fund || mongoose.model("Fund", FundSchema);
