import assert from "node:assert/strict";
import { test } from "node:test";
import { parseNum, stepNumber } from "../js/numeric.js";

test("decimal point and decimal comma preserve the same soil measurement", () => {
  assert.equal(parseNum("1.3"), 1.3);
  assert.equal(parseNum("1,3"), 1.3);
  assert.equal(parseNum("0.05"), 0.05);
  assert.equal(parseNum("0,05"), 0.05);
});

test("stepper output can be read again without multiplying the measurement", () => {
  const value = stepNumber(parseNum("1,3"), 0.1, 0, 20).toFixed(1);
  assert.equal(value, "1.4");
  assert.equal(parseNum(value), 1.4);
  assert.equal(stepNumber(parseNum(value), -0.1, 0, 20), 1.3);
});

test("stepper respects both limits and empty input", () => {
  assert.equal(stepNumber(20, 0.1, 0, 20), 20);
  assert.equal(stepNumber(0, -0.1, 0, 20), 0);
  assert.equal(stepNumber(parseNum(""), 0.1, 0, 20), 0.1);
});

test("invalid and mixed separators do not become partial measurements", () => {
  for (const value of ["", "—", "1.234,56", "1,2,3", "1.3abc", "Infinity"]) {
    assert.ok(Number.isNaN(parseNum(value)), value);
  }
  assert.equal(parseNum("0"), 0);
  assert.equal(parseNum(" 1,3 "), 1.3);
});
