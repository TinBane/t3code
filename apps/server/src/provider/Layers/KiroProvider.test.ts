import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import { KiroSettings } from "@t3tools/contracts";

import { checkKiroProviderStatus } from "./KiroProvider.ts";
import { writeFakeCli } from "../../testUtils/fakeCli.ts";

const decodeKiroSettings = Schema.decodeSync(KiroSettings);

const LOGGED_IN_WHOAMI = '{"accountType":"SocialGoogle","email":"dev@example.com"}\n';
const LIST_MODELS_OUTPUT = JSON.stringify({
  models: [
    { model_name: "auto", model_id: "auto", context_window_tokens: 1_000_000 },
    { model_name: "claude-sonnet-4.5", model_id: "claude-sonnet-4.5", rate_multiplier: 1.3 },
    { model_name: "claude-haiku-4.5", model_id: "claude-haiku-4.5", rate_multiplier: 0.4 },
  ],
});

it.layer(NodeServices.layer)("checkKiroProviderStatus", (it) => {
  type FakeOutput = { readonly stdout: string; readonly stderr?: string; readonly code?: number };
  // A stand-in for the Kiro CLI: `--version`, `whoami`, and `chat --list-models` print canned text.
  const writeFakeKiroCli = (input: {
    readonly whoami: FakeOutput;
    readonly models: FakeOutput;
  }) => {
    const outputs = JSON.stringify({
      "--version": { stdout: "kiro-cli 2.23.1\n" },
      whoami: input.whoami,
      chat: input.models,
    });
    return Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      return writeFakeCli({
        directory: yield* fs.makeTempDirectoryScoped({ prefix: "t3code-kiro-probe-" }),
        name: "kiro-cli",
        source: [
          `const output = ${outputs}[process.argv[2]] ?? { code: 1 };`,
          'process.stdout.write(output.stdout ?? "");',
          'process.stderr.write(output.stderr ?? "");',
          "process.exit(output.code ?? 0);",
        ].join("\n"),
      });
    });
  };

  it.effect("reports the binary as missing when the binary path does not resolve", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkKiroProviderStatus(
        decodeKiroSettings({ enabled: true, binaryPath: "/definitely/not/installed/kiro-cli" }),
      );
      expect(snapshot.installed).toBe(false);
      expect(snapshot.status).toBe("error");
      expect(snapshot.message).toMatch(/not installed|not on PATH|Failed to execute/);
    }),
  );

  it.effect("reports ready with the CLI's models when logged in", () =>
    Effect.gen(function* () {
      const snapshot = yield* Effect.scoped(
        Effect.gen(function* () {
          const binaryPath = yield* writeFakeKiroCli({
            whoami: { stdout: LOGGED_IN_WHOAMI },
            models: { stdout: LIST_MODELS_OUTPUT },
          });
          return yield* checkKiroProviderStatus(decodeKiroSettings({ enabled: true, binaryPath }));
        }),
      );
      expect(snapshot.status).toBe("ready");
      expect(snapshot.version).toBe("2.23.1");
      expect(snapshot.auth).toEqual({
        status: "authenticated",
        type: "cached_token",
        label: "Kiro account",
        email: "dev@example.com",
      });
      expect(snapshot.models.map((model) => model.slug)).toEqual([
        "auto",
        "claude-sonnet-4.5",
        "claude-haiku-4.5",
      ]);
    }),
  );

  it.effect("reports unauthenticated from whoami without listing models", () =>
    Effect.gen(function* () {
      const snapshot = yield* Effect.scoped(
        Effect.gen(function* () {
          const binaryPath = yield* writeFakeKiroCli({
            whoami: { stdout: "Not logged in\n", code: 1 },
            models: { stdout: "", stderr: "must not run", code: 9 },
          });
          return yield* checkKiroProviderStatus(decodeKiroSettings({ enabled: true, binaryPath }));
        }),
      );
      expect(snapshot.status).toBe("error");
      expect(snapshot.auth.status).toBe("unauthenticated");
      expect(snapshot.message).toContain("kiro-cli login");
      expect(snapshot.models.map((model) => model.slug)).toEqual(["auto"]);
    }),
  );

  it.effect("points at `kiro-cli setup` when the chat component cannot launch", () =>
    Effect.gen(function* () {
      const snapshot = yield* Effect.scoped(
        Effect.gen(function* () {
          const binaryPath = yield* writeFakeKiroCli({
            whoami: { stdout: LOGGED_IN_WHOAMI },
            models: {
              stdout: "",
              stderr: "error: failed to launch /Users/dev/.local/bin/kiro-cli-chat\n",
              code: 1,
            },
          });
          return yield* checkKiroProviderStatus(decodeKiroSettings({ enabled: true, binaryPath }));
        }),
      );
      expect(snapshot.status).toBe("error");
      expect(snapshot.installed).toBe(true);
      expect(snapshot.message).toContain("kiro-cli setup");
    }),
  );
});
