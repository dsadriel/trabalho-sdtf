import argparse

from common.utils import parse_port
from server.cluster import Node


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('port', type=parse_port)
    parser.add_argument('--local', action='store_true')
    parser.add_argument('--show-port', '-p', action='store_true')
    args = parser.parse_args()
    node = Node(args.port, args.local, args.show_port)
    node.interface.start()
    node.interface.print_startup(node.state.num_reqs, node.state.total_sum)
    try:
        node.run()
    except KeyboardInterrupt:
        pass
    finally:
        with node.cv:
            node.ready = False
            node.cancel_work()
        node.sock.close()
        node.group.close()
        node.interface.stop()


if __name__ == '__main__':
    main()
