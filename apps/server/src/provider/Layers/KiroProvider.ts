import {
  type KiroSettings,
  type ModelCapabilities,
  type ServerProviderAuth,
  type ServerProviderModel,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import {
  AUTH_PROBE_TIMEOUT_MS,
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ProviderProbeResult,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import { KIRO_DEFAULT_MODEL_ID, kiroCliFailure } from "../acp/KiroAcpSupport.ts";

const KIRO_PRESENTATION = {
  displayName: "Kiro",
  supportsConversationRollback: false,
  // T3's Plan mode is not wired to Kiro's planning agent yet.
  showInteractionModeToggle: false,
} as const;
const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

const VERSION_PROBE_TIMEOUT_MS = 4_000;

const KIRO_BUILT_IN_MODELS: ReadonlyArray<ServerProviderModel> = [
  {
    slug: KIRO_DEFAULT_MODEL_ID,
    name: "Auto",
    isCustom: false,
    isDefault: true,
    capabilities: EMPTY_CAPABILITIES,
  },
];

function kiroSnapshot(
  kiroSettings: KiroSettings,
  checkedAt: string,
  probe: ProviderProbeResult,
  builtInModels = KIRO_BUILT_IN_MODELS,
): ServerProviderDraft {
  return buildServerProvider({
    presentation: KIRO_PRESENTATION,
    enabled: kiroSettings.enabled,
    checkedAt,
    models: providerModelsFromSettings(
      builtInModels,
      kiroSettings.customModels,
      EMPTY_CAPABILITIES,
    ),
    probe,
  });
}

export const buildInitialKiroProviderSnapshot = (kiroSettings: KiroSettings) =>
  Effect.map(DateTime.now, (now) =>
    kiroSnapshot(kiroSettings, DateTime.formatIso(now), {
      installed: kiroSettings.enabled,
      version: null,
      status: "warning",
      auth: { status: "unknown" },
      message: kiroSettings.enabled
        ? "Checking Kiro CLI availability..."
        : "Kiro is disabled in T3 Code settings.",
    }),
  );

const KiroWhoami = Schema.Struct({
  accountType: Schema.optional(Schema.NullOr(Schema.String)),
  email: Schema.optional(Schema.NullOr(Schema.String)),
});
const decodeKiroWhoami = Schema.decodeUnknownOption(Schema.fromJsonString(KiroWhoami));

/**
 * Parses `kiro-cli whoami --format json`. Logged in, the CLI prints
 * `{"accountType":"SocialGoogle","email":"…"}`; logged out it prints the text
 * `Not logged in`.
 */
function parseKiroWhoamiOutput(output: string): ServerProviderAuth {
  const trimmed = output.trim();
  const decoded = decodeKiroWhoami(trimmed);
  if (Option.isSome(decoded)) {
    const email = decoded.value.email?.trim();
    return {
      status: "authenticated",
      type: "cached_token",
      label: "Kiro account",
      ...(email ? { email } : {}),
    };
  }
  return /not logged in/i.test(trimmed) ? { status: "unauthenticated" } : { status: "unknown" };
}

const KiroModelsCliOutput = Schema.Struct({
  models: Schema.Array(
    Schema.Struct({
      model_id: Schema.String,
      model_name: Schema.optional(Schema.NullOr(Schema.String)),
    }),
  ),
});
const decodeKiroModelsCliOutput = Schema.decodeUnknownOption(
  Schema.fromJsonString(KiroModelsCliOutput),
);

/** Parses `kiro-cli chat --list-models --format json`; `auto` stays the default. */
function parseKiroModelsCliOutput(output: string): ReadonlyArray<ServerProviderModel> {
  const decoded = decodeKiroModelsCliOutput(output.trim());
  if (Option.isNone(decoded)) return [];
  const seen = new Set<string>();
  return decoded.value.models.flatMap((model): ServerProviderModel[] => {
    const slug = model.model_id.trim();
    if (!slug || seen.has(slug)) return [];
    seen.add(slug);
    return [
      {
        slug,
        name: slug === KIRO_DEFAULT_MODEL_ID ? "Auto" : model.model_name?.trim() || slug,
        isCustom: false,
        ...(slug === KIRO_DEFAULT_MODEL_ID ? { isDefault: true } : {}),
        capabilities: EMPTY_CAPABILITIES,
      },
    ];
  });
}

const runKiroCliCommand = (
  kiroSettings: KiroSettings,
  args: ReadonlyArray<string>,
  environment: NodeJS.ProcessEnv,
) =>
  Effect.gen(function* () {
    const command = kiroSettings.binaryPath || "kiro-cli";
    const spawnCommand = yield* resolveSpawnCommand(command, args, { env: environment });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: environment,
        shell: spawnCommand.shell,
      }),
    );
  });

/**
 * Health check from three CLI commands that never open a chat session:
 * `--version`, `whoami`, and `chat --list-models`. Opening a session would
 * record it under `~/.kiro/sessions` and start the agent's MCP servers.
 */
export const checkKiroProviderStatus = Effect.fn("checkKiroProviderStatus")(function* (
  kiroSettings: KiroSettings,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
  if (!kiroSettings.enabled) return yield* buildInitialKiroProviderSnapshot(kiroSettings);
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const failed = (version: string | null, message: string, probe?: Partial<ProviderProbeResult>) =>
    kiroSnapshot(kiroSettings, checkedAt, {
      installed: true,
      version,
      status: "error",
      auth: { status: "unknown" },
      message,
      ...probe,
    });
  // Output of a probe that ran to completion, or undefined if it failed or timed out.
  const probeCli = (args: ReadonlyArray<string>) =>
    runKiroCliCommand(kiroSettings, args, environment).pipe(
      Effect.timeout(AUTH_PROBE_TIMEOUT_MS),
      Effect.option,
      Effect.map(Option.getOrUndefined),
    );

  const versionResult = yield* runKiroCliCommand(kiroSettings, ["--version"], environment).pipe(
    Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS),
    Effect.result,
  );
  if (Result.isFailure(versionResult)) {
    const missing = isCommandMissingCause(versionResult.failure);
    yield* Effect.logWarning("Kiro CLI health check failed.", {
      errorTag: versionResult.failure._tag,
    });
    return missing
      ? failed(null, "Kiro CLI (`kiro-cli`) is not installed or not on PATH.", { installed: false })
      : failed(null, "Failed to execute Kiro CLI health check.");
  }
  if (Option.isNone(versionResult.success)) {
    return failed(null, "Kiro CLI is installed but timed out while running `kiro-cli --version`.");
  }
  const versionOutput = versionResult.success.value;
  const version = parseGenericCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);
  if (versionOutput.code !== 0) {
    yield* Effect.logWarning("Kiro CLI version probe exited with a non-zero status.", {
      exitCode: versionOutput.code,
    });
    return failed(version, "Kiro CLI is installed but failed to run.");
  }

  const whoami = yield* probeCli(["whoami", "--format", "json"]);
  if (!whoami) yield* Effect.logWarning("Kiro CLI whoami probe failed or timed out.");
  // Logged-out installs exit non-zero, so the text is read whatever the code.
  const auth: ServerProviderAuth = whoami
    ? parseKiroWhoamiOutput(`${whoami.stdout}\n${whoami.stderr}`)
    : { status: "unknown" };
  const setupFailure = whoami ? kiroCliFailure(whoami.stderr) : undefined;
  if (auth.status === "unauthenticated" || setupFailure !== undefined) {
    const message =
      setupFailure?.message ?? "Kiro CLI is installed but not logged in. Run `kiro-cli login`.";
    return failed(version, message, { auth });
  }

  const listing = yield* probeCli(["chat", "--list-models", "--format", "json"]);
  const listingFailure = listing ? kiroCliFailure(listing.stderr) : undefined;
  if (listingFailure !== undefined) {
    return failed(version, listingFailure.message, {
      auth: listingFailure.kind === "not-logged-in" ? { status: "unauthenticated" } : auth,
    });
  }
  const cliModels = listing?.code === 0 ? parseKiroModelsCliOutput(listing.stdout) : [];
  if (cliModels.length > 0) {
    return kiroSnapshot(
      kiroSettings,
      checkedAt,
      { installed: true, version, status: "ready", auth },
      cliModels,
    );
  }
  yield* Effect.logWarning("Kiro CLI model listing failed or timed out.");
  // A failed model listing degrades the model picker, it does not make chats fail.
  return kiroSnapshot(kiroSettings, checkedAt, {
    installed: true,
    version,
    status: "warning",
    auth,
    message: "Kiro CLI is installed but listing models failed. Model options may be incomplete.",
  });
});
