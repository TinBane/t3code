import type {
  OrchestrationV2ContextHandoff,
  OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { ContextHandoffBudgetError } from "@t3tools/provider-core/server/failure";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  historyCost,
  renderHistory,
  selectHistory,
} from "@t3tools/provider-core/server/handoffBudget";

function pendingContextHandoffs(input: {
  readonly handoffs: ReadonlyArray<OrchestrationV2ContextHandoff>;
  readonly providerThread: OrchestrationV2ProviderThread;
}) {
  const nativeThreadId = input.providerThread.nativeThreadRef?.nativeId ?? undefined;
  return input.handoffs.filter(
    (handoff) =>
      nativeThreadId === undefined ||
      handoff.delivery?.nativeThreadId !== nativeThreadId ||
      handoff.delivery.status === "pending",
  );
}

export function planContextHandoffDelivery(input: {
  readonly handoffs: ReadonlyArray<OrchestrationV2ContextHandoff>;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly budget: number;
  readonly alreadyDeliveredItemIds: ReadonlySet<string>;
}) {
  const nativeThreadId = input.providerThread.nativeThreadRef?.nativeId ?? undefined;
  const pending = pendingContextHandoffs(input);
  if (pending.length === 0) return { status: "empty" as const };
  let coverage = pending
    .map(
      (handoff) =>
        `Context handoff (${handoff.strategy === "fork_delta_summary" ? "merge_back / fork_delta_summary" : handoff.strategy}):\n${
          handoff.history?.coverage ??
          `From thread ${handoff.threadId}, runs ${handoff.coveredRunOrdinals.from}-${handoff.coveredRunOrdinals.to}. Recover history with t3_thread_read, view=activity; paginate with afterPosition, and use itemId/textOffset for long items.`
        }`,
    )
    .join("\n");
  if (historyCost([], coverage) > Math.min(4_000, input.budget / 2)) {
    const strategies = Array.from(new Set(pending.map((handoff) => handoff.strategy)));
    coverage = `Context handoff (${strategies.join(", ")}). ${pending.length} handoff records; detailed coverage references omitted. Recover history with t3_thread_read({threadId:"${input.providerThread.appThreadId ?? pending[0]!.threadId}",view:"activity",limit:20,maxCharsPerItem:4000}); paginate with afterPosition=nextPosition. Follow fork/handoff source references in activity. For long items use itemId and textOffset=nextTextOffset until null.`;
  }
  const seen = new Set(input.alreadyDeliveredItemIds);
  const messages = pending
    .flatMap((handoff) => handoff.history?.messages ?? [])
    .filter((message) => {
      if (seen.has(message.itemId)) return false;
      seen.add(message.itemId);
      return true;
    });
  const oldContext = pending
    .filter((handoff) => handoff.history === undefined)
    .map((handoff) => handoff.summaryText)
    .join("\n\n");
  const fullCoverage =
    oldContext && historyCost([], `${coverage}\n${oldContext}`) + 512 <= input.budget
      ? `${coverage}\n${oldContext}`
      : coverage;
  const selected = selectHistory({
    messages,
    coverage: fullCoverage,
    omittedItems: pending.reduce((sum, handoff) => sum + (handoff.history?.omittedItems ?? 0), 0),
    budget: input.budget,
  });
  return historyCost(selected.messages, selected.context) > input.budget
    ? { status: "overflow" as const }
    : { status: "ready" as const, pending, selected, nativeThreadId };
}

/**
 * Persist before/after injection: an ambiguous pending delivery requires a fresh native thread.
 * Run `unsent` when the provider refused the turn before reading the prompt, so that
 * refusal does not leave an ambiguous marker behind.
 */
export const deliverContextHandoffs = Effect.fn("orchestrationV2.deliverContextHandoffs")(
  function* <InjectError = never, PersistError = never, BudgetError = never>(input: {
    readonly handoffs: ReadonlyArray<OrchestrationV2ContextHandoff>;
    readonly providerThread: OrchestrationV2ProviderThread;
    readonly budget: number | Effect.Effect<number, BudgetError>;
    readonly deferInline?: boolean;
    readonly alreadyDeliveredItemIds: ReadonlySet<string>;
    readonly inject?: (
      history: ProviderAdapter.ProviderAdapterV2HistoricalContext,
    ) => Effect.Effect<boolean, InjectError>;
    readonly persist: (handoff: OrchestrationV2ContextHandoff) => Effect.Effect<void, PersistError>;
  }) {
    if (
      pendingContextHandoffs(input).length === 0 ||
      (input.deferInline && input.inject === undefined)
    )
      return { context: "", delivered: Effect.void, unsent: Effect.void };
    const budget = typeof input.budget === "number" ? input.budget : yield* input.budget;
    const plan = planContextHandoffDelivery({
      handoffs: input.handoffs,
      providerThread: input.providerThread,
      budget,
      alreadyDeliveredItemIds: input.alreadyDeliveredItemIds,
    });
    if (plan.status === "empty")
      return { context: "", delivered: Effect.void, unsent: Effect.void };
    if (plan.status === "overflow") {
      if (input.deferInline) return { context: "", delivered: Effect.void, unsent: Effect.void };
      return yield* new ContextHandoffBudgetError();
    }
    const { nativeThreadId, pending, selected } = plan;
    const omittedItemIds = new Set(selected.omittedItemIds);
    const persist = (status: "pending" | "injected" | "inline") =>
      Effect.forEach(
        pending,
        (handoff) =>
          input.persist({
            ...handoff,
            ...(nativeThreadId === undefined
              ? {}
              : {
                  delivery: {
                    nativeThreadId,
                    status,
                    omittedItemIds: [
                      ...(handoff.history?.omittedItemIds ?? []),
                      ...(handoff.history?.messages ?? [])
                        .filter((message) => omittedItemIds.has(message.itemId))
                        .map((message) => message.itemId),
                    ],
                    itemIds: selected.messages
                      .filter((message) =>
                        handoff.history?.messages.some(
                          (candidate) => candidate.itemId === message.itemId,
                        ),
                      )
                      .map((message) => message.itemId),
                  },
                }),
          }),
        { discard: true },
      );
    if (input.inject !== undefined && nativeThreadId !== undefined) {
      if (
        pending.some(
          (handoff) =>
            handoff.delivery?.nativeThreadId === nativeThreadId &&
            handoff.delivery.status === "pending",
        )
      ) {
        return yield* new ContextHandoffDeliveryUncertainError();
      }
      yield* persist("pending");
      const injected = yield* input.inject({
        messages: selected.messages,
        context: selected.context,
      });
      if (injected) {
        yield* persist("injected");
        return { context: "", delivered: Effect.void, unsent: Effect.void };
      }
    } else {
      yield* persist("pending");
    }
    if (input.deferInline) {
      // Compaction APIs cannot accept an inline transcript. Keep it available
      // for the next ordinary turn when native injection is unsupported.
      yield* Effect.forEach(pending, input.persist, { discard: true });
      return { context: "", delivered: Effect.void, unsent: Effect.void };
    }
    return {
      context: renderHistory(selected.messages, selected.context),
      delivered: persist("inline"),
      unsent: Effect.forEach(pending, input.persist, { discard: true }),
    };
  },
);

export class ContextHandoffDeliveryUncertainError extends Schema.TaggedError<ContextHandoffDeliveryUncertainError>()(
  "ContextHandoffDeliveryUncertainError",
  {},
) {
  override get message() {
    return "Historical context delivery is uncertain; replace the native thread before retrying.";
  }
}
