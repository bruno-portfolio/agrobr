import assert from "node:assert/strict";
import { test } from "node:test";
import { parseNum } from "../js/numeric.js";

test("decimal point and decimal comma preserve the same soil measurement", () => {
  assert.equal(parseNum("1.3"), 1.3);
  assert.equal(parseNum("1,3"), 1.3);
  assert.equal(parseNum("0.05"), 0.05);
  assert.equal(parseNum("0,05"), 0.05);
});

test("invalid and mixed separators do not become partial measurements", () => {
  for (const value of ["", "—", "1.234,56", "1,2,3", "1.3abc", "Infinity"]) {
    assert.ok(Number.isNaN(parseNum(value)), value);
  }
  assert.equal(parseNum("0"), 0);
  assert.equal(parseNum(" 1,3 "), 1.3);
});
