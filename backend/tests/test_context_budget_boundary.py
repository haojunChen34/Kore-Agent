import unittest

from kore_next.context import ContextManager


class StepBudgetBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_complete_round_between_working_target_and_ceiling_survives(self):
        manager = ContextManager(None)
        messages = [
            {"role": "system", "content": "s" * 6100},
            {"role": "assistant", "content": None,
             "tool_calls": [{"id": "call-1", "type": "function", "function": {
                 "name": "artifact_publish", "arguments": '{"path":"report.md"}'}}]},
            {"role": "tool", "tool_call_id": "call-1", "content": "r" * 2500},
        ]
        async def unexpected_summary(*args):
            self.fail("No older prefix should be summarized")

        result = await manager.prepare_step(
            messages=messages, provider="openai", model="fixture",
            tool_schema_tokens=3333, context_window_override=8192,
            summarizer=unexpected_summary,
        )
        self.assertGreater(result.estimated_tokens, result.working_input_budget)
        self.assertLessEqual(result.estimated_tokens, result.max_input_tokens)
        self.assertEqual(result.messages, messages)
        self.assertFalse(result.summary_updated)
        self.assertEqual(result.dropped_messages, 0)

    async def test_current_round_over_hard_ceiling_is_still_rejected(self):
        manager = ContextManager(None)
        with self.assertRaisesRegex(ValueError, "current tool round exceeds"):
            await manager.prepare_step(
                messages=[{"role": "system", "content": "s" * 13000},
                          {"role": "user", "content": "Keep the current instruction"}],
                provider="openai", model="fixture", tool_schema_tokens=3333,
                context_window_override=8192,
            )


if __name__ == "__main__":
    unittest.main()
