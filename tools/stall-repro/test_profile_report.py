import unittest

from profile_report import responsibility


class ProfileAttributionTests(unittest.TestCase):
    def test_membership_inside_flip_is_not_assigned_to_sequencer(self):
        self.assertEqual(responsibility("propagate_one;MembershipCircuit::contains;dbsp::seek"), "flip_walk_including_requery_evaluation")

    def test_mutex_guard_symbol_does_not_turn_sampler_into_lock_cpu(self):
        self.assertEqual(responsibility("mem::spawn_sampler;tokio::sync::mutex::MutexGuard;mem_totals"), "memory_sampler")

    def test_io_client_work_is_separated_from_owning_walk(self):
        self.assertEqual(responsibility("propagate_one;tokio_postgres::query"), "postgres_client")


if __name__ == "__main__":
    unittest.main()
