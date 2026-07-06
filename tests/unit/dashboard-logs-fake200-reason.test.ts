import { describe, expect, it } from "vitest";

import { getFake200ReasonKey } from "@/app/[locale]/dashboard/logs/_components/fake200-reason";

describe("fake200 reason mapping", () => {
  it("maps OpenAI Responses failed fake-200 code to a concrete i18n key", () => {
    expect(getFake200ReasonKey("FAKE_200_OPENAI_RESPONSE_FAILED", "fake200Reasons")).toBe(
      "fake200Reasons.openAIResponseFailed"
    );
  });

  it("keeps unknown suffix for unmapped fake-200 codes", () => {
    expect(getFake200ReasonKey("FAKE_200_SOMETHING_NEW", "fake200Reasons")).toBe(
      "fake200Reasons.unknown"
    );
  });
});
