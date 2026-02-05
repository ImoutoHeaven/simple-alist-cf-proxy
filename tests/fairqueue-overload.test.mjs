import { test } from 'node:test';
import assert from 'node:assert/strict';
import { nextOverloadDelayMs } from '../src/fairqueue-overload.js';

test('overload backoff increases by 500ms up to 4s', () => {
  assert.equal(nextOverloadDelayMs(0), 1000);
  assert.equal(nextOverloadDelayMs(1), 1500);
  assert.equal(nextOverloadDelayMs(2), 2000);
  assert.equal(nextOverloadDelayMs(6), 4000);
});
