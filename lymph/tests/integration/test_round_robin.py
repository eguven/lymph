from kazoo.client import KazooClient
from kazoo.handlers.gevent import SequentialGeventHandler

from lymph.core.decorators import rpc
from lymph.core.interfaces import Interface
from lymph.discovery.zookeeper import ZookeeperServiceRegistry
from lymph.events.null import NullEventSystem
from lymph.testing import LymphIntegrationTestCase


class Foo(Interface):
    service_type = 'foo'

    @rpc()
    def foo(self):
        return 'bar'


class RoundRobinTest(LymphIntegrationTestCase):
    use_zookeeper = True

    def setUp(self):
        super(RoundRobinTest, self).setUp()
        self.events = NullEventSystem()

        self.instance_count = 3
        self.endpoints = [self.create_container(Foo, 'foo')[0].endpoint for i in range(self.instance_count)]
        self.lymph_client = self.create_client()

    def create_registry(self, **kwargs):
        zkclient = KazooClient(self.hosts, handler=SequentialGeventHandler())
        return ZookeeperServiceRegistry(zkclient)

    def test_pick_instance_round_robin(self):
        request_count = 7
        sorted_endpoints = sorted(self.endpoints)
        expected_order = (
            (request_count / self.instance_count) * sorted_endpoints +
            sorted_endpoints[:(request_count % self.instance_count)]
        )
        actual_order = [self.lymph_client.request('foo', 'foo.foo', {}).source for i in range(request_count)]
        self.assertEqual(expected_order, actual_order)
