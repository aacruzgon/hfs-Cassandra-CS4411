// One benchmark operation against one HFS instance, driven by bench.py.
//
// Open loop (MODE=open): k6's constant-arrival-rate executor starts RATE
// iterations per second whether or not earlier ones have answered, on at most
// VUS virtual users. A saturated system therefore shows as latency and as
// dropped_iterations, not as a lower offered rate.
//
// Closed loop (MODE=closed): VUS virtual users, each sending its next request
// when the last one has answered. Used only to measure capacity for the
// calibration, and for the long saturating runs behind the LWT steady-state
// bins.
//
// Every number comes from k6's CSV output, which bench.py reduces to one row
// per request. Nothing in this script aggregates or classifies.

import http from 'k6/http';
import exec from 'k6/execution';
import { SharedArray } from 'k6/data';

const OP = __ENV.OP;
const BASE = __ENV.BASE_URL;
const VUS = Number(__ENV.VUS);
const DURATION = __ENV.DURATION;
const MODE = __ENV.MODE || 'open';
const RATE = Number(__ENV.RATE || 0);
const SEARCH_OPS = ['token', 'date', 'uri'];

// A request in flight when the window closes may finish for this long; its
// row is kept. Longer would stretch the run past its window.
const GRACEFUL_STOP = '10s';

const scenario =
  MODE === 'open'
    ? {
        executor: 'constant-arrival-rate',
        rate: RATE,
        timeUnit: '1s',
        duration: DURATION,
        preAllocatedVUs: VUS,
        maxVUs: VUS,
        gracefulStop: GRACEFUL_STOP,
      }
    : { executor: 'constant-vus', vus: VUS, duration: DURATION, gracefulStop: GRACEFUL_STOP };

export const options = {
  discardResponseBodies: true,
  scenarios: { load: scenario },
};

const HEADERS = {
  'Content-Type': 'application/fhir+json',
  Accept: 'application/fhir+json',
};

// Each array is built once and shared, read-only, by every virtual user.
const LINES = (name, path) =>
  new SharedArray(name, () => open(path).split('\n').filter((line) => line.length > 0));

const ids = OP === 'read' ? LINES('ids', __ENV.IDS_FILE) : null;
const bodies = OP === 'create' || OP === 'update' ? LINES('bodies', __ENV.BODIES_FILE) : null;
const queries = SEARCH_OPS.includes(OP)
  ? new SharedArray('queries', () => JSON.parse(open(__ENV.SEARCH_FILE))[OP])
  : null;

// A prime step visits the array in a scattered but fixed order, so both
// backends see the same sequence and no two requests touch one id before the
// sequence wraps around. Reduced first so the product stays an exact integer.
const STEP = 16777619;
function pick(array, n) {
  const length = array.length;
  return array[((n % length) * (STEP % length)) % length];
}

export function setup() {
  return { hot: `bench-hot-${Date.now()}` };
}

export default function (data) {
  const n = exec.scenario.iterationInTest;
  const params = { headers: HEADERS, tags: { name: OP }, timeout: '60s' };

  if (OP === 'read') {
    http.get(`${BASE}/${pick(ids, n)}`, params);
  } else if (OP === 'create') {
    // POST of one resource: the server assigns the id (roadmap Step 0, Spec).
    const resource = JSON.parse(pick(bodies, n));
    delete resource.id;
    http.post(`${BASE}/${resource.resourceType}`, JSON.stringify(resource), params);
  } else if (OP === 'update') {
    // Narrative is not a search parameter here, so an update writes a new
    // version without changing what the search indexes hold.
    const resource = JSON.parse(pick(bodies, n));
    resource.text = {
      status: 'generated',
      div: `<div xmlns="http://www.w3.org/1999/xhtml">benchmark update ${n}</div>`,
    };
    http.put(
      `${BASE}/${resource.resourceType}/${resource.id}`,
      JSON.stringify(resource),
      params,
    );
  } else if (SEARCH_OPS.includes(OP)) {
    http.get(`${BASE}/${pick(queries, n)}`, params);
  } else if (OP === 'hotkey') {
    // Unconditional PUTs to one id (roadmap Step 8, ADR-aacruzgon-0024).
    const resource = { resourceType: 'Patient', id: data.hot, active: true, birthDate: '1980-01-01' };
    http.put(`${BASE}/Patient/${data.hot}`, JSON.stringify(resource), params);
  } else {
    exec.test.abort(`unknown operation ${OP}`);
  }
}
