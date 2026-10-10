const GRAINS = ["soja", "milho", "feijão", "trigo"];

const CERRADO_V50 = {
  method: "v",
  v2: 50,
  name: "Embrapa Cerrados: saturação por bases 50%",
  target: "V = 50% na camada 0-20 (culturas de sequeiro no Cerrado)",
  sources: ["embrapaSoja2020", "embrapaTrigo2026"],
};

const CQFS_SMP = {
  method: "smp",
  phTarget: 6.0,
  name: "Manual RS/SC: índice SMP para pH 6,0",
  target: "pH em água 6,0 na camada 0-20",
  sources: ["cqfs2016"],
};

function v(v2, name, source) {
  return { method: "v", v2, name, target: `V = ${v2}% na camada 0-20`, sources: [source] };
}

function grains(config) {
  return Object.fromEntries(GRAINS.map(crop => [crop, config]));
}

export const MANUALS = {
  SP: {
    soja: v(70, "Boletim 100 (IAC): saturação por bases 70%", "embrapaSoja2020"),
    trigo: v(70, "Boletim 100 (IAC): saturação por bases 70%", "embrapaTrigo2026"),
  },
  PR: {
    soja: v(70, "Paraná: saturação por bases 70%", "embrapaSoja2020"),
    trigo: v(70, "Paraná: saturação por bases 70%", "embrapaTrigo2026"),
  },
  MS: {
    soja: v(60, "Mato Grosso do Sul: saturação por bases 60%", "embrapaSoja2020"),
    trigo: v(60, "Mato Grosso do Sul: saturação por bases 60%", "embrapaTrigo2026"),
  },
  MT: grains(CERRADO_V50),
  GO: grains(CERRADO_V50),
  DF: grains(CERRADO_V50),
  BA: grains(CERRADO_V50),
  MG: {
    ...grains(CERRADO_V50),
    café: {
      method: "alcamg",
      mt: 25,
      x: 3.5,
      name: "5ª Aproximação (MG): neutralização do Al e suprimento de Ca+Mg",
      target: "Saturação por Al até 25% e Ca+Mg de 3,5 cmolc/dm³ (cafeeiro)",
      sources: ["paye2019"],
    },
  },
  ES: {
    café: {
      method: "vctc",
      name: "Incaper: saturação por bases conforme a CTC (Guarçoni, 2017)",
      target: "V = 60%, 70% ou 80% conforme a CTC seja alta, média ou baixa",
      sources: ["paye2019"],
    },
  },
  RS: { ...grains(CQFS_SMP), pastagem: CQFS_SMP },
  SC: { ...grains(CQFS_SMP), pastagem: CQFS_SMP },
};

export function coffeeVeByCtc(ctc) {
  if (ctc > 8.6) return 60;
  if (ctc > 4.3) return 70;
  return 80;
}
