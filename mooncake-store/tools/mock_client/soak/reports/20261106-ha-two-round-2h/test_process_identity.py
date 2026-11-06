import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import node


class ProcessIdentityTests(unittest.TestCase):
    def test_register_waits_for_cmdline_after_exec(self):
        empty = {"pid": 123, "start_ticks": "456", "argv": [""]}
        ready = dict(empty, argv=["/test/bin", "--case=test"])
        with tempfile.TemporaryDirectory() as path:
            with patch.object(
                node, "identity", side_effect=[empty, ready]
            ), patch.object(node.time, "sleep"):
                self.assertEqual(node.register(Path(path), "master", 123), ready)


if __name__ == "__main__":
    unittest.main()
