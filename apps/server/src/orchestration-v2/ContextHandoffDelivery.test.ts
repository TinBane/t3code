import { assert, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";

import { deliverContextHandoffs } from "./ContextHandoffDelivery.ts";

it.effect("does not evaluate the budget when no handoff needs delivery", () =>
  Effect.gen(function* () {
    let budgetEvaluated = false;
    const now = DateTime.makeUnsafe("2026-01-01T00:00:00.000Z");
    const providerThread: OrchestrationV2ProviderThread = {
      id: ProviderThreadId.make("provider-thread:settled"),
      driver: ProviderDriverKind.make("acp"),
      providerInstanceId: ProviderInstanceId.make("kiro"),
      providerSessionId: ProviderSessionId.make("provider-session:settled"),
      appThreadId: ThreadId.make("thread:settled"),
      ownerNodeId: null,
      nativeThreadRef: {
        driver: ProviderDriverKind.make("acp"),
        nativeId: "native:settled",
        strength: "strong",
      },
      nativeConversationHeadRef: null,
      status: "idle",
      firstRunOrdinal: 1,
      lastRunOrdinal: 1,
      handoffIds: [],
      contextUsage: null,
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
    };

    yield* deliverContextHandoffs({
      handoffs: [],
      providerThread,
      budget: Effect.sync(() => {
        budgetEvaluated = true;
        return 0;
      }),
      alreadyDeliveredItemIds: new Set(),
      persist: () => Effect.void,
    });

    assert.isFalse(budgetEvaluated);
  }),
);
