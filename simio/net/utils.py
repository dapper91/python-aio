import socket


def get_socket_local_address(sock: socket.socket) -> tuple[str, int]:
    """
    Returns socket local address.

    :param sock: socket to get address from
    :return: socket local address
    """

    if sock.family is socket.AddressFamily.AF_INET:
        address, port = sock.getsockname()
    elif sock.family is socket.AddressFamily.AF_INET6:
        address, port, _, _ = sock.getsockname()
    else:
        raise ValueError("unexpected socket family")

    return address, port


def get_socket_peer_address(sock: socket.socket) -> tuple[str, int]:
    """
    Returns socket peer address.

    :param sock: socket to get address from
    :return: socket peer address
    """

    if sock.family is socket.AddressFamily.AF_INET:
        address, port = sock.getpeername()
    elif sock.family is socket.AddressFamily.AF_INET6:
        address, port, _, _ = sock.getpeername()
    else:
        raise ValueError("unexpected socket family")

    return address, port
