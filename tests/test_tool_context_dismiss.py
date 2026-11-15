"""Explicit provider-view dismissal must not mutate the durable archive."""

import copy
import unittest

from glassmoon.tool_context import ToolContext


class ToolContextDismissTests(unittest.TestCase):
    def setUp(self):
        # Dismissal is deliberately independent of archive IO and token counting.
        self.context = ToolContext.__new__(ToolContext)
        self.rounds = [
            ({"outputs": {"a": "first payload " * 1000,
                          "b": "second payload " * 1000}},
             {"a": 11, "b": 12}),
            ({"outputs": {"c": "third payload " * 1000}}, {"c": 13}),
        ]

    def test_only_selected_outputs_are_replaced(self):
        before = copy.deepcopy(self.rounds)
        receipt = self.context.dismiss(self.rounds, [11, 13, 11])
        self.assertEqual(receipt, {"dismissed": [11, 13]})
        self.assertEqual(self.rounds[0][0]["outputs"]["b"],
                         before[0][0]["outputs"]["b"])
        for index, call_id, archive_id in [(0, "a", 11), (1, "c", 13)]:
            output = self.rounds[index][0]["outputs"][call_id]
            self.assertIn(f"Archived tool result #{archive_id}", output)
            self.assertLess(len(output), len(before[index][0]["outputs"][call_id]))
            self.assertEqual(self.rounds[index][1], before[index][1])

    def test_invalid_selection_is_atomic(self):
        for selection in ([11, 999], [True], [0], [-1], ["11"], [], None, "11"):
            with self.subTest(selection=selection):
                before = copy.deepcopy(self.rounds)
                with self.assertRaises(ValueError):
                    self.context.dismiss(self.rounds, selection)
                self.assertEqual(self.rounds, before)

    def test_repeat_dismissal_is_idempotent(self):
        self.context.dismiss(self.rounds, [11])
        before = copy.deepcopy(self.rounds)
        self.assertEqual(self.context.dismiss(self.rounds, [11]), {"dismissed": [11]})
        self.assertEqual(self.rounds, before)


if __name__ == "__main__":
    unittest.main()
