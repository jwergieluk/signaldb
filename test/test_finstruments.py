import unittest
import finstruments
import copy


class SimplisticTest(unittest.TestCase):
    def test_check_instruments_for_fake_ones(self):
        instruments = finstruments.InstrumentFaker.get(3)
        for i in instruments:
            self.assertEqual(finstruments.check_instrument(i), 0)

    def test_consolidate_idempotence(self):
        """Consolidate returns unmodified input if it's already consolidated"""
        instruments = finstruments.InstrumentFaker.get(3)
        instrument_snapshot = copy.deepcopy(instruments)

        consolidated_instruments = finstruments.consolidate(instruments)
        self.assertEqual(instruments, instrument_snapshot)
        self.assertEqual(instruments, consolidated_instruments)
        self.assertEqual(instrument_snapshot, consolidated_instruments)
