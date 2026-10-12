// The deploy path through Vercel's own SDK (@vercel/sdk), against a running
// mirror. It validates every answer against the vendored document's schemas,
// which is the strictest reader of this API there is: if the emulator answers
// something the document would not, this throws.
//
//   MIRROR_URL=http://127.0.0.1:4566 node sdk-journey.mjs
//
// Run from a directory whose node_modules has @vercel/sdk (test/vercel-clients
// is not that directory; set MIRROR_VERCEL_CLIENT_DIR and the Go test runs this
// with it as the working directory). The emulator takes any token.
import assert from "node:assert/strict";
import { Vercel } from "@vercel/sdk";

const serverURL = process.env.MIRROR_URL ?? "http://127.0.0.1:4566";
const bearerToken = process.env.VERCEL_TOKEN ?? "mirror-dev-token";
const name = process.env.SDK_PROJECT ?? "sdk-journey";
const vercel = new Vercel({ bearerToken, serverURL });

const me = await vercel.user.getAuthUser();
assert.ok(me.user.id.startsWith("usr_"), "the caller has a user id");

const project = await vercel.projects.createProject({ requestBody: { name, framework: "vite" } });
assert.equal(project.name, name);

const env = await vercel.projects.createProjectEnv({
  idOrName: project.id,
  requestBody: { key: "API_URL", value: "https://api.example.test", type: "plain", target: ["production", "preview"] },
});
assert.ok(env.created, "the variable was created");

const deployment = await vercel.deployments.createDeployment({
  requestBody: {
    name,
    project: name,
    target: "production",
    files: [{ file: "index.html", data: "<h1>sdk</h1>" }],
  },
});
assert.ok(deployment.id.startsWith("dpl_"));

let state = deployment.readyState;
for (let i = 0; i < 30 && state !== "READY"; i++) {
  assert.ok(!["ERROR", "CANCELED"].includes(state), `the deployment ended ${state}`);
  await new Promise((resolve) => setTimeout(resolve, 1000));
  state = (await vercel.deployments.getDeployment({ idOrUrl: deployment.id })).readyState;
}
assert.equal(state, "READY");

const alias = await vercel.aliases.assignAlias({ id: deployment.id, requestBody: { alias: "sdk.example.test" } });
assert.equal(alias.alias, "sdk.example.test");

const domain = await vercel.projects.addProjectDomain({ idOrName: project.id, requestBody: { name: "www.sdk.example.test" } });
assert.equal(domain.verified, false);
assert.ok(domain.verification?.length, "an unverified domain carries the challenge to publish");

const config = await vercel.edgeConfig.createEdgeConfig({ requestBody: { slug: "sdk_journey", items: { greeting: "hello" } } });
assert.ok(config.id.startsWith("ecfg_"));
await vercel.edgeConfig.deleteEdgeConfig({ edgeConfigId: config.id });

await vercel.projects.deleteProject({ idOrName: project.id });
console.log(`sdk journey ok against ${serverURL}`);
