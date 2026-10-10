export const LAYERS = ["0-20", "20-40"];
export const GRAIN_CROPS = ["soja", "milho", "feijão", "trigo"];
export const SOUTH_STATES = ["PR", "RS", "SC"];

export const CA_SAT_TARGET = 0.60;
export const CAO_KG_PER_CMOLC = 560;
export const MGO_KG_PER_CMOLC = 400;
export const MG_CRITICAL = { "0-20": 2.0, "20-40": 1.0 };
export const CA_CRITICAL = { "0-20": 4.1, "20-40": 1.9 };
export const PAPER_CTC_RANGE = [3.1, 9.5];
export const PAPER_PRNT_RANGE = [77, 100];
export const PAPER_RATE_RANGE = [0, 20];

export const SPD_V_TARGET = 70;
export const SPD_REAPPLY_V = 50;

export const PARCEL_THRESHOLD_THA = 5.0;
export const HIGH_RATE_THA = 10.0;

export const GYPSUM_AL_SAT_TRIGGER = 20;
export const GYPSUM_CA_TRIGGER = 0.5;
export const GYPSUM_KG_PER_CLAY_PCT = 50;
export const GYPSUM_CA_ECEC_TARGET = 0.60;
export const GYPSUM_CA_ECEC_TRIGGER = 0.50;
export const GYPSUM_CAIRES_FACTOR = 6.4;

export const Y_CLAY_COEFFICIENTS = [0.0302, 0.06532, -0.000257];

export const SMP_TABLE = {
  "4.4": [15.0, 21.0, 29.0],
  "4.5": [12.5, 17.3, 24.0],
  "4.6": [10.9, 15.1, 20.0],
  "4.7": [9.6, 13.3, 17.5],
  "4.8": [8.5, 11.9, 15.7],
  "4.9": [7.7, 10.7, 14.2],
  "5.0": [6.6, 9.9, 13.3],
  "5.1": [6.0, 9.1, 12.3],
  "5.2": [5.3, 8.3, 11.3],
  "5.3": [4.8, 7.5, 10.4],
  "5.4": [4.2, 6.8, 9.5],
  "5.5": [3.7, 6.1, 8.6],
  "5.6": [3.2, 5.4, 7.8],
  "5.7": [2.8, 4.8, 7.0],
  "5.8": [2.3, 4.2, 6.3],
  "5.9": [2.0, 3.7, 5.6],
  "6.0": [1.6, 3.2, 4.9],
  "6.1": [1.3, 2.7, 4.3],
  "6.2": [1.0, 2.2, 3.7],
  "6.3": [0.8, 1.8, 3.1],
  "6.4": [0.6, 1.4, 2.6],
  "6.5": [0.4, 1.1, 2.1],
  "6.6": [0.2, 0.8, 1.6],
  "6.7": [0.0, 0.5, 1.2],
  "6.8": [0.0, 0.3, 0.8],
  "6.9": [0.0, 0.2, 0.5],
  "7.0": [0.0, 0.0, 0.2],
  "7.1": [0.0, 0.0, 0.0],
};
export const SMP_SPD_FRACTION = 0.25;
export const SMP_SPD_MAX_THA = 5.0;
export const SMP_SPD_SKIP_V_PCT = 65.0;
export const SMP_SPD_SKIP_AL_SAT_PCT = 10.0;

export const CA_RANGE = [0.0, 20.0];
export const MG_RANGE = [0.0, 10.0];
export const AL_RANGE = [0.0, 10.0];
export const H_AL_RANGE = [0.0, 30.0];
export const K_RANGE = [0.0, 2.0];
export const CLAY_RANGE = [0.0, 100.0];
export const PH_SMP_RANGE = [3.5, 8.0];
export const CAO_RANGE = [15.0, 56.0];
export const MGO_RANGE = [0.0, 25.0];
export const PRNT_RANGE = [45.0, 125.0];
export const K_MG_TO_CMOLC = 391.0;

export const CTC_HIGH_THRESHOLD = 40.0;
export const CAO_MGO_LEGAL_MIN = 38.0;
