import unittest
import copy
import signaldb


class SimplisticTest(unittest.TestCase):
    def test_consolidate_idempotence(self):
        instruments = signaldb.FinstrumentFaker.get(3)
        instrument_snapshot = copy.deepcopy(instruments)

        consolidated_instruments = signaldb.consolidate(instruments)
        self.assertEqual(instruments, instrument_snapshot)
        self.assertEqual(instruments, consolidated_instruments)
        self.assertEqual(instrument_snapshot, consolidated_instruments)
