"""Local handover prototype using the existing engine and ownership protocol.

The lab controller closes admission, obtains a fresh source receipt, quiesces A,
waits for both its exit and slot release, promotes B, waits for readiness, and
opens admission. There is no load balancer awaiting a readiness probe here, so
the existing two-second readiness advertising delay serves no purpose.

This is NOT a production recommendation to disable a load balancer drain. It
does not pre-fold catalogs, add automatic failure takeover, or change boot code.
"""
from hardening import Lab


class RollingLab(Lab):
    def spawn(self, name, argv, env=None, pipe=False):
        if name.startswith('engine-'):
            env = {**env, 'ELECTRIC_CIRCUITS_SHUTDOWN_DRAIN_SECS': '0'}
        return super().spawn(name, argv, env, pipe)
