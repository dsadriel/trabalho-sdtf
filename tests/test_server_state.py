import unittest

from server.state import ServerState


class TestServerState(unittest.TestCase):
    def setUp(self):
        self.state = ServerState()

    def test_initial_state(self):
        # Global state should start at zero
        self.assertEqual(self.state.num_reqs, 0)
        self.assertEqual(self.state.total_sum, 0)
        self.assertEqual(len(self.state.get_clients()), 0)

    def test_register_new_client(self):
        client = self.state.register_client("192.168.1.100")
        self.assertEqual(client.address, "192.168.1.100")
        self.assertEqual(client.last_req, 0)
        self.assertEqual(client.last_num_reqs, 0)
        self.assertEqual(client.last_total_sum, 0)

        # Check retrieval
        stored = self.state.get_client("192.168.1.100")
        self.assertIsNotNone(stored)
        self.assertEqual(stored, client)

        # Check with explicit port
        client_with_port = self.state.register_client("192.168.1.100", port=5000)
        self.assertEqual(client_with_port.address, "192.168.1.100:5000")

    def test_register_existing_client_does_not_overwrite(self):
        # Register a client and simulate progress
        self.state.register_client("192.168.1.100")
        client = self.state.get_client("192.168.1.100")
        client.last_req = 3
        client.last_num_reqs = 3
        client.last_total_sum = 45

        # Re-registering the same client must preserve their current state
        re_registered = self.state.register_client("192.168.1.100")
        self.assertEqual(re_registered.last_req, 3)
        self.assertEqual(re_registered.last_total_sum, 45)

    def test_remove_client_on_exit(self):
        self.state.register_client("192.168.1.100", port=4000)
        self.assertIsNotNone(self.state.get_client("192.168.1.100", port=4000))

        removed = self.state.remove_client("192.168.1.100", port=4000)
        self.assertIsNotNone(removed)
        self.assertIsNone(self.state.get_client("192.168.1.100", port=4000))

    def test_process_first_valid_request(self):
        self.state.register_client("1.1.1.2")
        # Client sends request #1 with value 10
        status, num_reqs, total_sum = self.state.process_request(
            address="1.1.1.2", id_req=1, value=10
        )

        self.assertEqual(status, "NORMAL")
        self.assertEqual(num_reqs, 1)
        self.assertEqual(total_sum, 10)

        # Check that server global state updated
        self.assertEqual(self.state.num_reqs, 1)
        self.assertEqual(self.state.total_sum, 10)

        # Check that client record updated
        client = self.state.get_client("1.1.1.2")
        self.assertEqual(client.last_req, 1)
        self.assertEqual(client.last_num_reqs, 1)
        self.assertEqual(client.last_total_sum, 10)

    def test_process_multiple_clients(self):
        # Simulating the exact scenario from Table 1 of the specification
        self.state.register_client("1.1.1.2")
        self.state.register_client("1.1.1.3")

        # Client 1.1.1.2 sends #1 (+9)
        self.state.process_request("1.1.1.2", id_req=1, value=9)
        # Client 1.1.1.2 sends #2 (+34) -> total: 43
        self.state.process_request("1.1.1.2", id_req=2, value=34)

        self.assertEqual(self.state.num_reqs, 2)
        self.assertEqual(self.state.total_sum, 43)

        # Client 1.1.1.3 sends #1 (+7) -> total: 50
        status, n_reqs, t_sum = self.state.process_request("1.1.1.3", id_req=1, value=7)
        self.assertEqual(status, "NORMAL")
        self.assertEqual(n_reqs, 3)
        self.assertEqual(t_sum, 50)

    def test_process_duplicate_request(self):
        self.state.register_client("1.1.1.2")

        # First request #1 (+10)
        self.state.process_request("1.1.1.2", id_req=1, value=10)

        # Retransmission of request #1 (+10)
        status, num_reqs, total_sum = self.state.process_request(
            "1.1.1.2", id_req=1, value=10
        )

        # Must detect duplicate, must not sum again!
        self.assertEqual(status, "DUPLICATE")
        self.assertEqual(num_reqs, 1)
        self.assertEqual(total_sum, 10)
        self.assertEqual(self.state.total_sum, 10)  # Still 10, not 20!
        self.assertEqual(self.state.num_reqs, 1)  # Still 1, not 2!

    def test_process_gap_request_jump(self):
        # Client registers and sends #1
        self.state.register_client("1.1.1.2")
        self.state.process_request("1.1.1.2", id_req=1, value=10)

        # Client now sends #3 (gap: skipped #2)
        status, num_reqs, total_sum = self.state.process_request(
            "1.1.1.2", id_req=3, value=50
        )

        # Specification: do not sum, respond with last acknowledged state
        self.assertEqual(status, "GAP")
        self.assertEqual(num_reqs, 1)  # Unchanged
        self.assertEqual(total_sum, 10)  # Unchanged (did NOT add 50)
        self.assertEqual(self.state.total_sum, 10)

    def test_unregistered_client_auto_registers(self):
        # If client sends request without discovery, auto-register and process
        status, num_reqs, total_sum = self.state.process_request(
            "10.0.0.1", id_req=1, value=5
        )
        self.assertEqual(status, "NORMAL")
        self.assertEqual(total_sum, 5)
        self.assertIsNotNone(self.state.get_client("10.0.0.1"))


if __name__ == "__main__":
    unittest.main()
